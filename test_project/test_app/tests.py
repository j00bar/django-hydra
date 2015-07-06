# -*- coding: utf-8 -*-
from __future__ import absolute_import

import logging

logger = logging.getLogger(__name__)

from django.conf import settings
from django.contrib.auth.models import User
from django.db import models, connections
from django.test import TestCase

from hydra import activate_branch, deactivate_branch
from hydra import models as hydra

from .models import Reader  #, Author, Book


class HydraInitializedTestCase(TestCase):
    def setUp(self):
        hydra.get_session_identifier_from_sequence(connection=connections['default'])
        for model in settings.HYDRA_MODELS:
            model_cls = models.get_model(*model.split('.', 1))
            hydra.initialize_model_for_hydra(model_cls)
        self.user = User.objects.create_user('jpschmoe',
                                             'joeschmoe@example.com',
                                             '12345')
        self.branch = hydra.Branch.objects.create(branch_name='test',
                                                  created_by=self.user)

    def test_simple_model_default_branch(self):
        # A simple insert/update/delete confirming that everything works as
        # normal.

        reader_obj = Reader.objects.create(name='Book Worm',
                                           email='bookworm@example.com')
        raw_reader_obj = hydra.HydraReader.objects.get(
            _branch_name__isnull=True,
            _id=reader_obj.id
        )
        raw_updated = raw_reader_obj._updated
        self.assert_(raw_updated)
        self.assertEqual(raw_reader_obj.name, reader_obj.name)
        self.assertEqual(raw_reader_obj.email, reader_obj.email)

        reader_obj.name = 'Big Worm'
        reader_obj.email = 'bigworm@example.com'
        reader_obj.save()

        raw_reader_obj = hydra.HydraReader.objects.get(
            _branch_name__isnull=True,
            _id=reader_obj.id
        )
        self.assertGreater(raw_reader_obj._updated, raw_updated)
        self.assertEqual(raw_reader_obj.name, reader_obj.name)
        self.assertEqual(raw_reader_obj.email, reader_obj.email)

        reader_obj.delete()
        self.assertRaises(hydra.HydraReader.objects.get,
                          _branch_name__isnull=True,
                          _id=reader_obj.id
        )

    def test_simple_model_custom_branch(self):
        # Start by creating a reader in the default branch
        reader_obj = Reader.objects.create(name='Book Worm',
                                           email='bookworm@example.com')
        reader_pk = reader_obj.pk
        default_raw_reader_obj = hydra.HydraReader.objects.get(
            _branch_name__isnull=True,
            _id=reader_obj.id
        )
        raw_updated = default_raw_reader_obj._updated
        self.assert_(raw_updated)
        self.assertEqual(default_raw_reader_obj.name, reader_obj.name)
        self.assertEqual(default_raw_reader_obj.email, reader_obj.email)

        # Now update that reader in the test branch and confirm that a modified
        # copy was made in the raw table
        activate_branch(self.branch)
        reader_obj = Reader.objects.get(pk=reader_pk)
        reader_obj.name = 'Big Worm'
        reader_obj.email = 'bigworm@example.com'
        reader_obj.save()
        default_raw_reader_obj = hydra.HydraReader.objects.get(
            _branch_name__isnull=True,
            _id=reader_obj.id
        )
        self.assertEqual(default_raw_reader_obj._updated, raw_updated)
        self.assertNotEqual(default_raw_reader_obj.name, reader_obj.name)
        self.assertNotEqual(default_raw_reader_obj.email, reader_obj.email)
        branch_raw_reader_obj = hydra.HydraReader.objects.get(
            _branch_name=self.branch.branch_name,
            _id=reader_obj.id
        )
        self.assertGreater(branch_raw_reader_obj._updated, raw_updated)
        self.assertEqual(branch_raw_reader_obj.name, reader_obj.name)
        self.assertEqual(branch_raw_reader_obj.email, reader_obj.email)

        # Now to test that changes aren't reflected in the default branch yet
        # We will delete the existing one and create a new one - both in the
        # branch - and then deactivate the branch to ensure that the changes
        # do not appear in the default branch
        reader_obj.delete()
        branch_reader_obj = Reader.objects.create(name='Little Tugger',
                                                  email='tugger@example.com')
        deactivate_branch()
        self.assertRaises(hydra.HydraReader.objects.get,
                          _branch_name__isnull=True,
                          _id=branch_reader_obj.id
        )
        self.assertRaises(Reader.objects.get,
                          pk=branch_reader_obj.pk)
        reader_obj = Reader.objects.get(pk=reader_pk)

        # Now let's make sure that the changes are still reflected in the
        # test branch
        activate_branch(self.branch)
        branch_raw_reader_obj = hydra.HydraReader.objects.get(
            _branch_name=self.branch.branch_name,
            _id=branch_reader_obj.id
        )
        self.assertEqual(branch_raw_reader_obj.name, branch_reader_obj.name)
        self.assertEqual(branch_raw_reader_obj.email, branch_reader_obj.email)
        self.assertRaises(
            Reader.objects.get,
            pk=reader_pk
        )
        branch_raw_reader_obj = hydra.HydraReader.objects.get(
            _branch_name=self.branch.branch_name,
            _id=reader_pk
        )
        self.assertGreater(branch_raw_reader_obj._updated, raw_updated)
        self.assert_(branch_raw_reader_obj._deleted)












