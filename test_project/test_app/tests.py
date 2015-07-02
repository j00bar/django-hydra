# -*- coding: utf-8 -*-
from __future__ import absolute_import

import logging

logger = logging.getLogger(__name__)

from django.conf import settings
from django.contrib.auth.models import User
from django.db import models
from django.test import TestCase

from hydra import activate_branch, deactivate_branch
from hydra import models as hydra

from .models import Reader, Author, Book


class HydraInitializedTestCase(TestCase):
    def setUp(self):
        for model in settings.HYDRA_MODELS:
            model_cls = models.get_model(*model.split('.', 1))
            hydra.initialize_model_for_hydra(model_cls)
        self.user = User.objects.create_user('jpschmoe',
                                             'joeschmoe@example.com',
                                             '12345')
        self.branch = hydra.Branch.objects.create(branch_name='test',
                                                  created_by=self.user)

    def test_no_branch_active(self):
        reader_obj = Reader.objects.create(name='Book Worm',
                                           email='bookworm@example.com')
        author_obj = Author.objects.create(name='Writer Dude',
                                           email='iwroteabook@example.com')
        raw_reader_obj = hydra.HydraReader.objects.get()
        raw_author_obj = hydra.HydraAuthor.objects.get()
        self.assertIsNone(raw_reader_obj.branch_name)
        self.assertIsNone(raw_author_obj.branch_name)
        self.assert_(raw_reader_obj._updated)
        self.assert_(raw_author_obj._updated)
        self.assertEqual(raw_reader_obj._id, reader_obj.pk)
        self.assertEqual(raw_author_obj._id, author_obj.pk)





