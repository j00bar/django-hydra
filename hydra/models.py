# -*- coding: utf-8 -*-
from __future__ import absolute_import

import logging

logger = logging.getLogger(__name__)

import sys

import django
from django.conf import settings
from django.db import models, router, connections, transaction, utils
if django.get_version() < (1,7):
    from django.db.models.signals import post_syncdb as post_migrate
else:
    from django.db.models.signals import post_migrate
from django.db.backends.signals import connection_created

from .utils import with_m2ms

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

def forbidden_models():
    from django.contrib.auth.models import User, Group, Permission
    from django.contrib.sites.models import Site
    from django.contrib.contenttypes.models import ContentType
    return {User, Group, Permission, Site, ContentType, Branch}


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
    for model_cls in with_m2ms(for_model) - forbidden_models():
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

        db_cur.execute("ALTER TABLE _raw_%(table)s "
                       "ADD COLUMN _id INTEGER NOT NULL, "
                       "ADD COLUMN _deleted BOOLEAN DEFAULT 'f', "
                       "ADD COLUMN _branch_name VARCHAR(50), "
                       "ADD COLUMN _updated TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP, "
                       "ADD CONSTRAINT %(table)s_branch_eff_id_uniq_tgthr UNIQUE (_id, _branch_name)"
                       "" % {'table': ModelCls._meta.db_table})

        # The raw table updated timestamp needs to be updated upon any write
        # because we need it to determine whether another branch has merged
        # and there are conflicts to resolve
        db_cur.execute("CREATE OR REPLACE FUNCTION _hail_hydra_%(table)s_update_timestamp() "
                       "RETURNS TRIGGER AS $$ "
                       "BEGIN "
                       "    NEW._updated = now(); "
                       "    RETURN NEW; "
                       "END; "
                       "$$ language 'plpgsql'"
                       "" % {'table': ModelCls._meta.db_table})
        db_cur.execute("CREATE TRIGGER _hail_hydra_%(table)s_update_timestamp "
                       "BEFORE UPDATE OR DELETE ON _raw_%(table)s FOR EACH ROW "
                       "EXECUTE PROCEDURE _hail_hydra_%(table)s_update_timestamp()"
                       "" % {'table': ModelCls._meta.db_table})

        # Create view over new table
        fields_except_pk = [field.column for field in ModelCls._meta.fields if not field.primary_key]
        db_cur.execute(
            'CREATE RULE "_RETURN" AS ON SELECT TO %(table)s DO INSTEAD '
            "SELECT id, %(fields)s FROM ("
            "SELECT _id AS id, %(fields)s, "
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
            "SET %(value_map)s "
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
            "SET _deleted = 't' "
            "WHERE _id = OLD.id AND "
            "_branch_name IS NOT DISTINCT FROM hydra_branch() "
            "RETURNING id, %(fields)s"
            "" % {'table': ModelCls._meta.db_table,
                  'fields': ', '.join(fields_except_pk)}
        )
