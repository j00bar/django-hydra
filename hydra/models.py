# -*- coding: utf-8 -*-
from __future__ import absolute_import

import logging

logger = logging.getLogger(__name__)

import sys

import django
from django.conf import settings
from django.db import models, router, connections, transaction, utils
from django.db.backends.postgresql_psycopg2.creation import DatabaseCreation
if django.get_version() < (1,7):
    from django.db.models.signals import post_syncdb as post_migrate
else:
    from django.db.models.signals import post_migrate
from django.db.backends.signals import connection_created

from .utils import with_m2ms, forbidden_models, is_hydrized

class Branch(models.Model):
    branch_name = models.CharField(max_length=50, unique=True)
    created_by = models.ForeignKey('auth.User')
    created = models.DateTimeField(auto_now_add=True)
    last_modified = models.DateTimeField(auto_now=True)
    state = models.CharField(
        max_length=6, default=u'open',
        choices=[(u'open', u'Open'),
                 (u'closed', u'Closed'),
                 (u'merged', u'Merged')])

    def __unicode__(self):
        return self.branch_name

class ActiveBranch(models.Model):
    session_id = models.BigIntegerField(primary_key=True)
    branch_name = models.CharField(max_length=50)

def after_hydra_migrate(sender=None, **kwargs):
    if django.get_version() >= (1,7):
        # After 1.7, this is an AppConfig, not a models module
        sender = sender.models_module
    if sender == models.get_app('hydra'):
        with transaction.atomic():
            db_cur = connections[(kwargs.get('using')
                                  if django.get_version() >= (1,7)
                                  else kwargs.get('db'))].cursor()
            db_cur.execute("SELECT count(*) FROM pg_class WHERE pg_class.relkind='S' "
                           "and pg_class.relname='_hydra_session_id_seq'")
            count, = db_cur.fetchone()
            if not count:
                db_cur.execute('CREATE SEQUENCE _hydra_session_id_seq NO CYCLE')
            # We need a view for outer joins during selects, but the function
            # can be used otherwise - with the STABLE modifier, we get some efficiency
            db_cur.execute("CREATE OR REPLACE VIEW _active_branch (branch_name) AS "
                           "SELECT branch_name FROM ("
                           " SELECT null::VARCHAR(50) AS branch_name UNION "
                           " SELECT branch_name FROM hydra_activebranch WHERE "
                           " session_id = currval('_hydra_session_id_seq')) sub "
                           "ORDER BY branch_name LIMIT 1")
            db_cur.execute("CREATE OR REPLACE FUNCTION hydra_branch() RETURNS VARCHAR(50) "
                           "AS $$ SELECT branch_name FROM _active_branch $$ LANGUAGE SQL STABLE ")
post_migrate.connect(after_hydra_migrate)


def get_session_identifier_from_sequence(sender=None, connection=None, **kwargs):
    """A sequence will be used to yield a unique identifier per session to set
    the active branch for this session."""
    db_cur = connection.cursor()
    try:
        db_cur.execute("SELECT nextval('_hydra_session_id_seq')")
    except utils.ProgrammingError:
        # The sequence hasn't been created yet... this is probably a syncdb/migrate
        pass
connection_created.connect(get_session_identifier_from_sequence)

def generate_raw_model_for(model_cls):
    # FIXME: Throw an error if there's a non-hydrized model with an FK to this one
    name = 'Hydra%s' % model_cls.__name__
    bases = (models.Model,)
    attrs = {'__module__': 'hydra',
             'Meta': type('Meta',
                          (object,),
                          {'app_label': 'hydra',
                           'db_table': '_raw_%s' % model_cls._meta.db_table,
                           'managed': False,
                           # FIXME: Carry over unique togethers from model Meta
                           'unique_together': [('_id', '_branch_name')]}),
             '_id': models.IntegerField(null=False),
             '_deleted': models.BooleanField(default=False),
             '_branch_name': models.CharField(max_length=50, null=True, db_index=True),
             '_updated': models.DateTimeField()}
    attrs.update(dict(**{f.attname: f for f in model_cls._meta.fields}))
    return type(name, bases, attrs)

def generate_hydra_models(for_model):
    for model_cls in with_m2ms(for_model) - forbidden_models(as_cls=True):
        setattr(models.get_app('hydra'),
                'Hydra%s' % model_cls.__name__,
                generate_raw_model_for(model_cls))

def initialize_model_for_hydra(ModelCls):
    db_conn = connections[router.db_for_write(ModelCls)]
    db_cur = db_conn.cursor()

    with transaction.atomic():
        db_cur.execute("SELECT COUNT(*) FROM pg_tables WHERE schemaname='public' AND "
                       "tablename = %s", ('_raw_%s' % ModelCls._meta.db_table,))
        result, = db_cur.fetchone()

        if result:
            logger.info('Model %s already initialized for Hydra', ModelCls)
            return

        # Rename existing table
        db_cur.execute('ALTER TABLE %(table)s RENAME TO _raw_%(table)s' %
                       {'table': ModelCls._meta.db_table})
        # FIXME: Find all indexes - single and multi-col, unique and not - and rewrite them to use branch_name

        # Create new table
        db_cur.execute("CREATE TABLE %(table)s ( LIKE _raw_%(table)s )" %
                       {'table': ModelCls._meta.db_table})

        db_cur.execute('CREATE SEQUENCE _raw_%(table)s__id_seq' %
                       {'table': ModelCls._meta.db_table})

        # Hydra fields need to be added
        # Unique index on branch + effective ID needs to be added
        db_cur.execute("ALTER TABLE _raw_%(table)s "
                       "ADD COLUMN _id INTEGER NOT NULL, "
                       "ADD COLUMN _deleted BOOLEAN DEFAULT 'f', "
                       "ADD COLUMN _branch_name VARCHAR(50) REFERENCES hydra_branch(branch_name), "
                       "ADD COLUMN _updated TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP, "
                       "ADD CONSTRAINT %(table)s_branch_eff_id_uniq_tgthr UNIQUE (_id, _branch_name)"
                       "" % {'table': ModelCls._meta.db_table})

        fields_except_pk = [field.column for field in ModelCls._meta.fields if not field.primary_key]

        # We have to implement referential integrity using triggers.
        # * No non-hydrized table will be allowed to reference a column in a
        #   hydrized table.
        # * Hydrized tables may contain foreign keys to non-hydrized tables
        #   and no triggers are needed.
        # * For hydrized rows referencing columns on other hydrized tables:
        #   * For INSERT and UPDATE forward consistency:
        #     * If the row is in the default branch, ensure there exists
        #       a row with the referenced column value in default
        #     * If the row is not in the default branch, ensure there exists
        #       a row with the referenced column value in the branch or default
        #   * For UPDATE backward consistency:
        #     * For any model that references an updated row, ensure that
        #       the referenced value is not changing in the update
        #   * For DELETE backward consistency:
        #     * For any model that references a deleted row, ensure that the
        #       delete cascades
        #
        # This leans heavily on the default-delete trigger.
        # Rows in default referencing something being deleted in
        # default should spawn non-deleted copies in every open branch before
        # being cascade deleted. If a row in a branch is deleted, any rows
        # in the branch referencing it should be deleted, and any rows in default
        # for which there is not a corresponding row in the branch should spawn
        # copies and be deleted.
        # DELETE trigger for rows in default
        # Before a row in default is deleted, it needs to spawn non-deleted copies
        # of itself for all other active branches.
        db_cur.execute(
            "CREATE OR REPLACE FUNCTION _hail_hydra_%(table)s_do_def_delete () "
            "RETURNS trigger AS "
            "$$ "
            "BEGIN "
            "IF NEW._deleted AND NOT OLD._deleted THEN "
            "INSERT INTO _raw_%(table)s "
            "(_id, _branch_name, %(fields)s) "
            "SELECT OLD._id, hydra_branch.branch_name AS _branch_name, %(old_fields)s "
            "FROM hydra_branch "
            "WHERE hydra_branch.state = 'open' AND NOT EXISTS ("
            "          SELECT 1 FROM _raw_%(table)s WHERE "
            "          _id = OLD.id AND _branch_name = hydra_branch.branch_name); "
            "END IF; "
            "RETURN NEW; "
            "END; "
            "$$ "
            "LANGUAGE plpgsql"
            "" % {'table': ModelCls._meta.db_table,
                  'fields': ', '.join(fields_except_pk),
                  'old_fields': ', '.join(['OLD.%s' % f for f in fields_except_pk])}
        )

        db_cur.execute(
            "CREATE TRIGGER _hail_hydra_%(table)s_def_delete_trgr BEFORE UPDATE "
            "ON _raw_%(table)s FOR EACH ROW WHERE _branch_name IS NULL "
            "DO _hail_hydra_to_def_delete()"
            "" % {'table': ModelCls._meta.db_table}
        )

        creator = DatabaseCreation(connections['default'])
        for f in ModelCls._meta.fields:
            if not isinstance(f, models.ForeignKey):
                continue
            related_model = f.rel.to
            if is_hydrized(related_model):
                # Foreign key constraints between hydrized tables need to be removed
                db_cur.execute("ALTER TABLE _raw_%(table)s "
                               "DROP CONSTRAINT %(table)s_%(column)s_fkey"
                               "" % {'table': ModelCls._meta.db_table,
                                     'column': f.column})
                # These triggers operate on the active branch, so no raw tables
                db_cur.execute("CREATE FUNCTION _hail_hydra_%(table)s_%(column)s_do_fk_fwd () "
                               "RETURNS trigger AS "
                               "$$ "
                               "BEGIN "
                               "SELECT 1 FROM %(rel_table)s WHERE "
                               "%(rel_column)s = NEW.%(column)s; "
                               "IF NOT FOUND THEN "
                               "    RAISE 'Foreign key constraint violation %(table)s.%(column)s -> %(rel_table).%(rel_column)s', "
                               "    USING ERRCODE = 'foreign_key_violation'; "
                               "ENDIF; "
                               "RETURN NEW;"
                               "END; "
                               "$$ "
                               "LANGUAGE plpgsql"
                               "" % {'table': ModelCls._meta.db_table,
                                     'column': f.column,
                                     'rel_table': f.rel.to._meta.db_table,
                                     'rel_column': f.rel.field_name}
                               )
                db_cur.execute("CREATE TRIGGER _hail_hydra_%(table)s_%(column)s_fk_fwd_trgr "
                               "BEFORE INSERT OR UPDATE ON %(table)s FOR EACH ROW "
                               "DO _hail_hydra_%(table)s_%(column)s_do_fk_fwd()"
                               "" % {'table': ModelCls._meta.db_table,
                                     'column': f.column})

        # Create view over new table
        db_cur.execute(
            'CREATE RULE "_RETURN" AS ON SELECT TO %(table)s DO INSTEAD '
            "SELECT id, %(fields)s FROM ("
            "SELECT _id AS id, %(fields)s, _deleted, "
            "row_number() OVER (PARTITION BY _id ORDER BY _branch_name) AS _row "
            "FROM _raw_%(table)s "
            "WHERE (_branch_name IS NULL OR _branch_name = hydra_branch())) subq "
            "WHERE _row = 1  AND _deleted = 'f'"
            "" % {'fields': ', '.join(fields_except_pk),
                  'table': ModelCls._meta.db_table}
        )

        # INSERT rule
        db_cur.execute(
            "CREATE RULE _hail_hydra_insert AS ON INSERT TO %(table)s DO INSTEAD "
            "INSERT INTO _raw_%(table)s "
            "(_id, _branch_name, %(fields)s) "
            "(SELECT nextval('_raw_%(table)s__id_seq') _id, hydra_branch() _branch_name, %(vals)s) "
            "RETURNING _id AS id, %(fields)s"
            "" % {'table': ModelCls._meta.db_table,
                  'fields': ', '.join(fields_except_pk),
                  'vals': ', '.join(['NEW.%s' % col for col in fields_except_pk])}
        )

        db_cur.execute(
            "CREATE RULE _hail_hydra_update AS ON UPDATE TO %(table)s "
            "DO INSTEAD ("
            "INSERT INTO _raw_%(table)s "
            "(_id, _branch_name, %(fields)s) "
            "SELECT _id, hydra_branch(), %(fields)s "
            "FROM _raw_%(table)s "
            "WHERE hydra_branch() IS NOT NULL AND _id = OLD.id "
            "      AND _branch_name IS NULL AND NOT EXISTS ("
            "          SELECT 1 FROM _raw_%(table)s WHERE "
            "          _id = OLD.id AND _branch_name = hydra_branch()); "
            "UPDATE _raw_%(table)s "
            "SET %(value_map)s, _updated = statement_timestamp() "
            "WHERE _id = OLD.id AND "
            "_branch_name IS NOT DISTINCT FROM hydra_branch() "
            "RETURNING _id as id, %(fields)s)"
            "" % {'table': ModelCls._meta.db_table,
                  'value_map': ', '.join(['%(col)s = NEW.%(col)s' % {'col': col}
                                          for col in fields_except_pk]),
                  'fields': ', '.join(fields_except_pk)}
        )

        # DELETE rule
        # A delete sets the deleted flag
        db_cur.execute(
            "CREATE RULE _hail_hydra_delete AS ON DELETE TO %(table)s DO INSTEAD "
            "UPDATE _raw_%(table)s "
            "SET _deleted = 't', _updated = statement_timestamp() "
            "WHERE _id = OLD.id AND "
            "_branch_name IS NOT DISTINCT FROM hydra_branch() "
            "RETURNING id, %(fields)s"
            "" % {'table': ModelCls._meta.db_table,
                  'fields': ', '.join(fields_except_pk)}
        )

