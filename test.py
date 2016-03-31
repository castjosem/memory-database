from simple_database import Database
from simple_database import Data
from simple_database import TransactionHandler
import unittest


class TestDatabase(unittest.TestCase):

	def setUp(self):
		self.database = Database()

	def test_set_value(self):
		self.database.set('a', '1')

	def test_set_value_check_data(self):
		final_data = {'a': '1'}
		final_values_freq = {'1': 1}
		
		self.database.set('a', '1')

		self.assertEqual(final_data, self.database.database.data)
		self.assertEqual(final_values_freq, self.database.database.values_freq)



	def test_get_key(self):
		self.database.get('a')

	def test_get_key_found(self):
		final_output = '1'		
		self.database.set('b', '1')
		output = self.database.get('b')
		self.assertEqual(output, final_output)

	def test_get_key_not_found(self):
		final_output = None	
		self.database.set('b', '1')
		output = self.database.get('a')
		self.assertEqual(output, final_output)


	def test_get_key_found_check_data(self):
		final_data = {'a': '1', 'z': '30'}
		final_values_freq = {'1': 1, '30': 1}
		final_output = '30'
		
		self.database.set('a', '1')
		self.database.set('z', '30')

		output = self.database.get('b')

		self.assertIsNone(output, final_output)
		self.assertEqual(final_data, self.database.database.data)
		self.assertEqual(final_values_freq, self.database.database.values_freq)


	def test_get_key_not_found_check_data(self):
		final_data = {'a': '1', 'z': '30'}
		final_values_freq = {'1': 1, '30': 1}
		final_output = '30'

		self.database.set('a', '1')
		self.database.set('z', '30')

		output = self.database.get('z')

		self.assertEqual(output, final_output)
		self.assertEqual(final_data, self.database.database.data)
		self.assertEqual(final_values_freq, self.database.database.values_freq)

	def test_begin(self):
		self.database.begin()

	def test_rollback_pass(self):
		self.database.begin()
		result = self.database.rollback()
		self.assertTrue(result)

	def test_rollback_fail(self):
		result = self.database.rollback()
		self.assertFalse(result)

	def test_can_rollback_(self):
		transaction_data = {'cc': ['9999'], 'bb': ['9999']}
		transaction_values_freq = {'9999': 2}

		final_data = {'a': '1', 'z': '30'}
		final_values_freq = {'1': 1, '30': 1}
				
		self.database.set('a', '1')
		self.database.set('z', '30')

		self.database.begin()
		self.database.set('bb', '9999')
		self.database.set('cc', '9999')

		self.assertEqual(transaction_data, self.database.transaction_handler.transactions.data)
		self.assertEqual(transaction_values_freq, self.database.transaction_handler.transactions.values_freq)

		self.database.rollback()

		self.assertEqual(final_data, self.database.database.data)
		self.assertEqual(final_values_freq, self.database.database.values_freq)


	
	def test_can_commit(self):
		
		commands = [
			('set', ['a', '0'], None),
			('set', ['b', '0'], None),
			('begin', [], None),
			('num_equal_to', ['0'], 2),
			('unset', ['f'], None),
			('set', ['a', '10'], None),
			('unset', ['b'], None),
			('set', ['z', '100'], None),
			('commit', [], True)
		]
		final_data = {'z': '100', 'a': '10'}
		final_values_freq = {'10': 1, '100': 1}

		for (method, arguments, output) in commands:
			func = getattr(self.database, method)
			result = func(*arguments)
			self.assertEqual(result, output)

		self.assertEqual(final_data, self.database.database.data)
		self.assertEqual(final_values_freq, self.database.database.values_freq)

if __name__ == '__main__':
	unittest.main()

