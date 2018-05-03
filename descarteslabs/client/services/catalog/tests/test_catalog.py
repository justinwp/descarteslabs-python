# Copyright 2018 Descartes Labs.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest
from copy import deepcopy
from time import sleep
from random import randint
from hashlib import md5

from descarteslabs.client.auth import Auth
from descarteslabs.client.services.catalog import Catalog

descartes_auth = Auth()


@unittest.skipIf(
    'descartes:team' not in descartes_auth.payload['groups'],
    'user is unauthorized for these endpoints')
class TestCatalog(unittest.TestCase):
    instance = None

    def setUp(self):
        product_id = 'test_product:{}'.format(md5(str(randint(0, 2**32))).hexdigest())
        self.instance = Catalog()
        self.product = {
            'title': 'Test Product',
            'description': 'A test product',
            'native_bands': ['red'],
            'orbit': 'sun-synchronous',
        }
        self.band = {
            'name': 'red',
            'wavelength_min': 700,
            'wavelength_max': 750,
            'srcfile': 0,
            'srcband': 1,
            'jpx_layer': 1,
            'dtype': 'Byte',
            'data_range': [0, 255],
            'nbits': 8,
            'type': 'spectral',
        }
        self.image = {
            'bucket': 'dl-storage-{}-data'.format(descartes_auth.namespace),
            'directory': 'sub_path',
            'files': ['/path/to/file.jp2'],
            'geometry': {
                "type": "Polygon",
                "coordinates": [
                    [
                        [
                            68.961181640625,
                            50.17689812200107,
                            0.0
                        ],
                        [
                            70.15869140625,
                            50.17689812200107,
                            0.0
                        ],
                        [
                            70.15869140625,
                            50.80593472676908,
                            0.0
                        ],
                        [
                            68.961181640625,
                            50.80593472676908,
                            0.0
                        ],
                        [
                            68.961181640625,
                            50.17689812200107,
                            0.0
                        ]
                    ]
                ]
            }
        }
        r = self.instance.add_product(product_id, **self.product)
        self.product_id = r['data']['id']

    def tearDown(self):
        sleep(1)
        self.instance.remove_product(self.product_id)

    def test_get_product(self):
        r = self.instance.get_product(self.product_id)
        self.assertEqual(r['data']['id'], self.product_id)

    def test_change_product(self):
        self.instance.change_product(self.product_id, **{'read': ['some_group']})

    def test_replace_product(self):
        product = deepcopy(self.product)
        product['description'] = 'A new description for this product'
        self.instance.replace_product(self.product_id, **product)

    def test_add_band(self):
        self.instance.add_band(self.product_id, 'red', **self.band)
        sleep(1)
        self.instance.get_band(self.product_id, 'red')
        self.instance.remove_band(self.product_id, 'red')

    def test_change_band(self):
        self.instance.add_band(self.product_id, 'red', **self.band)
        sleep(1)
        self.instance.change_band(self.product_id, 'red', read=['some_group'])
        self.instance.remove_band(self.product_id, 'red')

    def test_replace_band(self):
        self.instance.add_band(self.product_id, 'red', **self.band)
        sleep(1)

        band = deepcopy(self.band)
        band['srcfile'] = 1

        self.instance.replace_band(self.product_id, 'red', **band)
        self.instance.remove_band(self.product_id, 'red')

    def test_add_image(self):
        self.instance.add_image(self.product_id, 'some_meta_key', **self.image)
        sleep(1)
        self.instance.get_image(self.product_id, 'some_meta_key')
        self.instance.remove_image(self.product_id, 'some_meta_key')

    def test_change_image(self):
        self.instance.add_image(self.product_id, 'some_meta_key', **self.image)
        sleep(1)
        self.instance.change_image(self.product_id, 'some_meta_key', read=['some_group'])
        self.instance.remove_image(self.product_id, 'some_meta_key')

    def test_replace_image(self):
        self.instance.add_image(self.product_id, 'some_meta_key', **self.image)
        sleep(1)

        image = deepcopy(self.image)
        image['cloud_fraction'] = .5

        self.instance.replace_image(self.product_id, 'some_meta_key', **image)
        self.instance.remove_image(self.product_id, 'some_meta_key')

    def test_own_products(self):
        op = self.instance.own_products()
        self.assertGreater(len(op), 0)
        for p in op:
            self.assertEqual(p['owner']['uuid'], descartes_auth.payload['sub'])


if __name__ == '__main__':
    unittest.main()
