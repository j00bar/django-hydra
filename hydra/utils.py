# -*- coding: utf-8 -*-
from __future__ import absolute_import

import logging

logger = logging.getLogger(__name__)

import collections

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
