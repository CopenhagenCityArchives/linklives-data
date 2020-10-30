import unittest
from unittest.mock import MagicMock, patch
from index import PersonAppearance, csv_pa_bulk_actions, csv_pas_bulk_actions, csv_read_pas


class TestPersonAppearance(unittest.TestCase):

    def test_from_dict_field_parsed(self):
        pa = PersonAppearance.from_dict({'id': 12345, 'source_id': 1, 'source_year': 1845, 'name': 'Bo Larsen'})
        self.assertEqual(pa.name, 'Bo Larsen')
    
    def test_from_dict_unique_id(self):
        pa = PersonAppearance.from_dict({'id': 12345, 'source_id': 1, 'source_year': 1845, 'name': 'Bo Larsen'})
        self.assertEqual(pa.id, '1845-12345')
    
    def test_from_dict_missing_none(self):
        pa = PersonAppearance.from_dict({'id': 12345, 'source_id': 1, 'source_year': 1845, 'name': 'Bo Larsen'})
        self.assertIsNone(pa.birth_place)
    
    def test_from_dict_unknown_field_no_raise_invalid(self):
        pa = PersonAppearance.from_dict({'id': 12345, 'source_id': 1, 'source_year': 1845, 'name': 'Bo Larsen', 'unknown_field': 'unknown_value'})
        with self.assertRaises(AttributeError):
            getattr(pa, 'unknown_field')
    
    def test_from_dict_unknown_field_raise_invalid(self):
        with self.assertRaises(AttributeError):
            PersonAppearance.from_dict({'id': 12345, 'source_id': 1, 'source_year': 1845, 'name': 'Bo Larsen', 'unknown_field': 'unknown_value'}, raise_invalid=True)
    
    def test_from_dict_missing_id(self):
        with self.assertRaises(KeyError):
            PersonAppearance.from_dict({'source_id': 1, 'source_year': 1845, 'name': 'Bo Larsen'})
    
    def test_from_dict_all_valid_fields(self):
        d = {
            'id': 12345,
            'gender': 'a',
            'gender_clean': 'b',
            'gender_std': 'c',
            'age': 'd',
            'age_clean': 1.0,
            'name': 'd',
            'name_clean': 'e',
            'name_std': 'f',
            'first_names': 'g',
            'family_names': 'h',
            'patronyms': 'i',
            'uncat_names': 'j',
            'maiden_family_names': 'k',
            'maiden_patronyms': 'l',
            'all_possible_family_names': 'm',
            'all_possible_patronyms': 'n',
            'marital_status': 'o',
            'marital_status_clean': 'p',
            'marital_status_std': 'q',
            'household_position': 'r',
            'household_position_std': 's',
            'household_family_no': 't',
            'hh_id': 2,
            'occupation': 'u',
            'place_name': 'v',
            'land_register_address': 'w',
            'land_register': 'x',
            'address': 'y',
            'full_address': 'z',
            'parish': 'æ',
            'parish_type': 'ø',
            'district': 'å',
            'county': '1',
            'state_region': '2',
            'transcription_code': '3',
            'transcription_id': 3,
            'birth_place': '4',
            'birth_place_clean': '5',
            'birth_place_parish': '6',
            'birth_place_district': '7',
            'birth_place_county': '8',
            'birth_place_koebstad': '9',
            'birth_place_town': '10',
            'birth_place_place': '11',
            'birth_place_island': '12',
            'birth_place_other': '13',
            'birth_place_parish_std': '14',
            'birth_place_county_std': '15',
            'birth_place_koebstad_std': '16',
            'source_reference': '17',
            'transcriber_comments': '18',
            'source_year': 4,
            'event_type': '19',
            'role': '20'
        }
        
        pa = PersonAppearance.from_dict(d)
        self.assertEqual(pa.id, '4-12345')
        self.assertEqual(pa.pa_id, 12345)

        self.assertEqual(pa.gender, 'a',)
        self.assertEqual(pa.gender_clean, 'b',)
        self.assertEqual(pa.gender_std, 'c',)
        self.assertEqual(pa.age, 'd',)
        self.assertEqual(pa.age_clean, 1.0,)
        self.assertEqual(pa.name, 'd',)
        self.assertEqual(pa.name_clean, 'e',)
        self.assertEqual(pa.name_std, 'f',)
        self.assertEqual(pa.first_names, 'g',)
        self.assertEqual(pa.family_names, 'h',)
        self.assertEqual(pa.patronyms, 'i',)
        self.assertEqual(pa.uncat_names, 'j',)
        self.assertEqual(pa.maiden_family_names, 'k',)
        self.assertEqual(pa.maiden_patronyms, 'l',)
        self.assertEqual(pa.all_possible_family_names, 'm',)
        self.assertEqual(pa.all_possible_patronyms, 'n',)
        self.assertEqual(pa.marital_status, 'o',)
        self.assertEqual(pa.marital_status_clean, 'p',)
        self.assertEqual(pa.marital_status_std, 'q',)
        self.assertEqual(pa.household_position, 'r',)
        self.assertEqual(pa.household_position_std, 's',)
        self.assertEqual(pa.household_family_no, 't',)
        self.assertEqual(pa.hh_id, 2,)
        self.assertEqual(pa.occupation, 'u',)
        self.assertEqual(pa.place_name, 'v',)
        self.assertEqual(pa.land_register_address, 'w',)
        self.assertEqual(pa.land_register, 'x',)
        self.assertEqual(pa.address, 'y',)
        self.assertEqual(pa.full_address, 'z',)
        self.assertEqual(pa.parish, 'æ',)
        self.assertEqual(pa.parish_type, 'ø',)
        self.assertEqual(pa.district, 'å',)
        self.assertEqual(pa.county, '1',)
        self.assertEqual(pa.state_region, '2',)
        self.assertEqual(pa.transcription_code, '3',)
        self.assertEqual(pa.transcription_id, 3,)
        self.assertEqual(pa.birth_place, '4',)
        self.assertEqual(pa.birth_place_clean, '5',)
        self.assertEqual(pa.birth_place_parish, '6',)
        self.assertEqual(pa.birth_place_district, '7',)
        self.assertEqual(pa.birth_place_county, '8',)
        self.assertEqual(pa.birth_place_koebstad, '9',)
        self.assertEqual(pa.birth_place_town, '10',)
        self.assertEqual(pa.birth_place_place, '11',)
        self.assertEqual(pa.birth_place_island, '12',)
        self.assertEqual(pa.birth_place_other, '13',)
        self.assertEqual(pa.birth_place_parish_std, '14',)
        self.assertEqual(pa.birth_place_county_std, '15',)
        self.assertEqual(pa.birth_place_koebstad_std, '16',)
        self.assertEqual(pa.source_reference, '17',)
        self.assertEqual(pa.transcriber_comments, '18',)
        self.assertEqual(pa.source_year, 4,)
        self.assertEqual(pa.event_type, '19',)
        self.assertEqual(pa.role, '20')
    
    def test_from_dict_es_document_relation(self):
        d = {
            'id': '12345',
            'gender': 'a',
            'gender_clean': 'b',
            'gender_std': 'c',
            'age': 'd',
            'age_clean': '1.0',
            'name': 'd',
            'name_clean': 'e',
            'name_std': 'f',
            'first_names': 'g',
            'family_names': 'h',
            'patronyms': 'i',
            'uncat_names': 'j',
            'maiden_family_names': 'k',
            'maiden_patronyms': 'l',
            'all_possible_family_names': 'm',
            'all_possible_patronyms': 'n',
            'marital_status': 'o',
            'marital_status_clean': 'p',
            'marital_status_std': 'q',
            'household_position': 'r',
            'household_position_std': 's',
            'household_family_no': 't',
            'hh_id': '2',
            'occupation': 'u',
            'place_name': 'v',
            'land_register_address': 'w',
            'land_register': 'x',
            'address': 'y',
            'full_address': 'z',
            'parish': 'æ',
            'parish_type': 'ø',
            'district': 'å',
            'county': '1',
            'state_region': '2',
            'transcription_code': '3',
            'transcription_id': '3',
            'birth_place': '4',
            'birth_place_clean': '5',
            'birth_place_parish': '6',
            'birth_place_district': '7',
            'birth_place_county': '8',
            'birth_place_koebstad': '9',
            'birth_place_town': '10',
            'birth_place_place': '11',
            'birth_place_island': '12',
            'birth_place_other': '13',
            'birth_place_parish_std': '14',
            'birth_place_county_std': '15',
            'birth_place_koebstad_std': '16',
            'source_reference': '17',
            'transcriber_comments': '18',
            'source_year': '1990',
            'event_type': '19',
            'role': '20'
        }

        pa = PersonAppearance.from_dict(d)
        d['pa_id'] = int(pa.pa_id)
        d['id'] = pa.id
        d['source_id'] = int(pa.source_id)

        d['age_clean'] = float(d['age_clean'])
        d['source_year'] = int(d['source_year'])
        d['hh_id'] = int(d['hh_id'])
        d['transcription_id'] = int(d['transcription_id'])
        
        d['first_names'] = [d['first_names']]
        d['patronyms'] = [d['patronyms']]
        d['family_names'] = [d['family_names']]
        d['uncat_names'] = [d['uncat_names']]
        d['maiden_family_names'] = [d['maiden_family_names']]
        d['maiden_patronyms'] = [d['maiden_patronyms']]
        d['all_possible_patronyms'] = [d['all_possible_patronyms']]
        d['all_possible_family_names'] = [d['all_possible_family_names']]
        
        self.assertEqual(pa.es_document(), d)


class TestElasticSearchHelpers(unittest.TestCase):

    def test_csv_pa_bulk_action_no_links_no_life_courses(self):
        iterator = csv_pa_bulk_actions(PersonAppearance(123, 1), [], [])

        action = next(iterator)
        self.assertEqual(action.get('_op_type'), 'index')
        self.assertEqual(action.get('_index'), 'pas')
        self.assertEqual(action.get('_id'), '1-123')
        self.assertEqual(action.get('person_appearance').get('pa_id'), 123)

        with self.assertRaises(StopIteration):
            next(iterator)
    
    def test_csv_pa_bulk_action(self):
        iterator = csv_pa_bulk_actions(PersonAppearance(123, 1), [2], [3])

        action = next(iterator)
        self.assertEqual(action.get('_op_type'), 'index')
        self.assertEqual(action.get('_index'), 'pas')
        self.assertEqual(action.get('_id'), '1-123')
        self.assertEqual(action.get('person_appearance').get('pa_id'), 123)

        action = next(iterator)
        self.assertEqual(action.get('_op_type'), 'update')
        self.assertEqual(action.get('_index'), 'links')
        self.assertEqual(action.get('_id'), 3)
        self.assertIn('script', action.keys())
        self.assertEqual(action.get('script').get('params').get('pa').get('pa_id'), 123)

        action = next(iterator)
        self.assertEqual(action.get('_op_type'), 'update')
        self.assertEqual(action.get('_index'), 'lifecourses')
        self.assertEqual(action.get('_id'), 2)
        self.assertIn('script', action.keys())
        self.assertEqual(action.get('script').get('params').get('pa').get('pa_id'), 123)

        with self.assertRaises(StopIteration):
            next(iterator)

    def test_csv_pas_bulk_actions(self):
        pas = [(PersonAppearance(123, 1), [1], [1]), (PersonAppearance(234, 2), [1], [1, 2]), (PersonAppearance(345, 3), [1], [2])]
        iterator = csv_pas_bulk_actions(pas)
        
        action = next(iterator)
        self.assertEqual(action.get('_op_type'), 'index')
        self.assertEqual(action.get('_index'), 'pas')
        self.assertEqual(action.get('_id'), '1-123')
        self.assertEqual(action.get('person_appearance').get('pa_id'), 123)

        action = next(iterator)
        self.assertEqual(action.get('_op_type'), 'update')
        self.assertEqual(action.get('_index'), 'links')
        self.assertEqual(action.get('_id'), 1)
        self.assertIn('script', action.keys())
        self.assertEqual(action.get('script').get('params').get('pa').get('pa_id'), 123)

        action = next(iterator)
        self.assertEqual(action.get('_op_type'), 'update')
        self.assertEqual(action.get('_index'), 'lifecourses')
        self.assertEqual(action.get('_id'), 1)
        self.assertIn('script', action.keys())
        self.assertEqual(action.get('script').get('params').get('pa').get('pa_id'), 123)

        action = next(iterator)
        self.assertEqual(action.get('_op_type'), 'index')
        self.assertEqual(action.get('_index'), 'pas')
        self.assertEqual(action.get('_id'), '2-234')
        self.assertEqual(action.get('person_appearance').get('pa_id'), 234)

        action = next(iterator)
        self.assertEqual(action.get('_op_type'), 'update')
        self.assertEqual(action.get('_index'), 'links')
        self.assertEqual(action.get('_id'), 1)
        self.assertIn('script', action.keys())
        self.assertEqual(action.get('script').get('params').get('pa').get('pa_id'), 234)

        action = next(iterator)
        self.assertEqual(action.get('_op_type'), 'update')
        self.assertEqual(action.get('_index'), 'links')
        self.assertEqual(action.get('_id'), 2)
        self.assertIn('script', action.keys())
        self.assertEqual(action.get('script').get('params').get('pa').get('pa_id'), 234)

        action = next(iterator)
        self.assertEqual(action.get('_op_type'), 'update')
        self.assertEqual(action.get('_index'), 'lifecourses')
        self.assertEqual(action.get('_id'), 1)
        self.assertIn('script', action.keys())
        self.assertEqual(action.get('script').get('params').get('pa').get('pa_id'), 234)

        action = next(iterator)
        self.assertEqual(action.get('_op_type'), 'index')
        self.assertEqual(action.get('_index'), 'pas')
        self.assertEqual(action.get('_id'), '3-345')
        self.assertEqual(action.get('person_appearance').get('pa_id'), 345)

        action = next(iterator)
        self.assertEqual(action.get('_op_type'), 'update')
        self.assertEqual(action.get('_index'), 'links')
        self.assertEqual(action.get('_id'), 2)
        self.assertIn('script', action.keys())
        self.assertEqual(action.get('script').get('params').get('pa').get('pa_id'), 345)

        action = next(iterator)
        self.assertEqual(action.get('_op_type'), 'update')
        self.assertEqual(action.get('_index'), 'lifecourses')
        self.assertEqual(action.get('_id'), 1)
        self.assertIn('script', action.keys())
        self.assertEqual(action.get('script').get('params').get('pa').get('pa_id'), 345)

        with self.assertRaises(StopIteration):
            next(iterator)

class TestCsvFileHelpers(unittest.TestCase):

    def test_csv_read_pas_single_csv_no_life_courses_no_links(self):
        csv1 = MagicMock()
        csv1.open = unittest.mock.mock_open(read_data="id$source_year$name\n123$1845$Mads")
        
        iterator = csv_read_pas([csv1], {}, {})

        (pa, [], []) = next(iterator)
        self.assertEqual(pa.pa_id, '123')
        self.assertEqual(pa.source_year, '1845')
        self.assertEqual(pa.name, 'Mads')
        self.assertIsNone(pa.birth_place)

        with self.assertRaises(StopIteration):
            next(iterator)

    def test_csv_read_pas_empty_values_none(self):
        csv1 = MagicMock()
        csv1.open = unittest.mock.mock_open(read_data="id$source_year$birth_place$name\n123$1845$landsbylille$")

        (pa, _, _) = next(csv_read_pas([csv1], {}, {}))

        self.assertIsNone(pa.name)
    
    def test_csv_read_pas_multi_csv(self):
        csv1 = MagicMock()
        csv1.open = unittest.mock.mock_open(read_data="id$source_year$birth_place\n123$1845$landsbylille")
        csv2 = MagicMock()
        csv2.open = unittest.mock.mock_open(read_data="id$source_year$first_names\n234$1850$lars ole")

        pa_life_courses = {
            ('123', '1845'): [2]
        }

        pa_links = {
            ('234', '1850'): [1],
            ('123', '1845'): [2, 3]
        }

        iterator = csv_read_pas([csv1, csv2], pa_life_courses, pa_links)

        (pa, lcs, lis) = next(iterator)
        self.assertEqual(pa.pa_id, '123')
        self.assertEqual(pa.source_year, '1845')
        self.assertIsNone(pa.first_names)
        self.assertEqual(pa.birth_place, 'landsbylille')
        self.assertListEqual(lcs, [2])
        self.assertListEqual(lis, [2, 3])

        (pa, lcs, lis) = next(iterator)
        self.assertEqual(pa.pa_id, '234')
        self.assertEqual(pa.source_year, '1850')
        self.assertEqual(pa.first_names, 'lars ole')
        self.assertIsNone(pa.birth_place)
        self.assertListEqual(lcs, [])
        self.assertListEqual(lis, [1])

        with self.assertRaises(StopIteration):
            next(iterator)

if __name__ == '__main__':
    unittest.main()