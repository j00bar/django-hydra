# -*- coding: utf-8 -*-
from __future__ import absolute_import

import logging

logger = logging.getLogger(__name__)

from django.conf import settings
from django.db.models.signals import class_prepared

def activate_branch(branch_obj):
    if not branch_obj.state == u'open':
        raise ValueError('Only open branches can be activated.')
    from django.db import transaction, connections
    cursor = connections['default'].cursor()
    with transaction.atomic():
        cursor.execute("DELETE FROM hydra_activebranch WHERE "
                       "session_id = currval('_hydra_session_id_seq')")
        cursor.execute("INSERT INTO hydra_activebranch (session_id, branch_name) "
                       "VALUES (currval('_hydra_session_id_seq'), %s)",
                       (branch_obj.branch_name,))

def deactivate_branch():
    from django.db import connections
    cursor = connections['default'].cursor()
    cursor.execute("DELETE FROM hydra_activebranch WHERE "
                   "session_id = currval('_hydra_session_id_seq')")

_registered = set()
def hydrize_model(sender=None, **kwargs):
    logger.debug('Model %s is ready.', sender)
    if (sender not in _registered and
                ('%s.%s' % (sender._meta.app_label, sender._meta.model_name)).lower()
                in [s.lower() for s in getattr(settings, 'HYDRA_MODELS', [])]):
        logger.info('Generating Hydra models for %s', sender)
        from .models import generate_hydra_models
        generate_hydra_models(sender)
    _registered.add(sender)
class_prepared.connect(hydrize_model)
