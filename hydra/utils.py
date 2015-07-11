# -*- coding: utf-8 -*-
from __future__ import absolute_import

import logging

logger = logging.getLogger(__name__)

import collections

from django.conf import settings
from django.db import models

def is_hydrized(model):
    model = model_ref(model) if issubclass(model, Model) else model
    return model in settings.HYDRA_MODELS

def forbidden_models(as_cls=False):
    to_return = [
        'auth.User',
        'auth.Group',
        'auth.Permission',
        'contenttypes.ContentType',
        'sites.Site'
    ]
    if as_cls:
        to_return = map(lambda model_ref: models.get_model(*model_ref.split('.', 1)),
                        to_return)
    return set(to_return)


def model_ref(model_cls):
    return u'%s.%s' % (model_cls._meta.app_label, model_cls._meta.model_name)

def flatten(iter):
    """Flattens an iterable of items, such that any nested lists, sets, dicts,
    or generators are also flattened."""
    for item in iter:
        if isinstance(item, collections.Iterable) and not isinstance(item, basestring):
            for sub in flatten(item):
                yield sub
        else:
            yield item

def with_m2ms(model_cls):
    """Returns a set of models with their m2m "through" tables"""
    s = set([model_cls])
    try:
        return s | {
            o for o in flatten(
                [
                    [f.rel.through for f in m._meta.many_to_many]
                    for m in s
                ]
            )
        }
    except Exception, e:
        import pdb; pdb.set_trace()
