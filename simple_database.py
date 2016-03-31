"""
Simple Database Challenge

Name: Jose Castillo



Functionality:

    SET name value
        Set the variable name to the value value. Neither variable names nor values will contain spaces.
    GET name
        Print out the value of the variable name, or NULL if that variable is not set.
    UNSET name
        Unset the variable name, making it just like that variable was never set.
    NUMEQUALTO value
        Print out the number of variables that are currently set to value. If no variables equal that value, print 0.
    END
        Exit the program.

    In addition to the above data commands, it supports database transactions by also implementing these commands:

    BEGIN
        Open a new transaction block. Transaction blocks can be nested; a BEGIN can be issued inside of an existing block.
    ROLLBACK
        Undo all of the commands issued in the most recent transaction block, and close the block. 
        Print nothing if successful, or print NO TRANSACTION if no transaction is in progress.
    COMMIT
        Close all open transaction blocks, permanently applying the changes made in them. 
        Print nothing if successful, or print NO TRANSACTION if no transaction is in progress.

    Any data command that is run outside of a transaction block should commit immediately


Memory usage:
    In order to save memory while reducing running time, transactions are handled by a 
    'key - value list'  structure, where constant time can be performed and only modified 
    keys are stored.

    Data storage are separate for the database and transaction blocks in order to keep
    consistency. The database is only touched if there are no open transactions.

    Console -> Database <-> Transaction Handler

"""




class Database(object):
    """Simple In-Memory Database similar to Redis

    Attributes:
        database: an object of type Data that stores the current key-value pairs of the database
                  this object is not modified if a transaction is opened, keeping it consistent

        transaction_handler: ab object of type TransactionHandler that handles all methods related 
                             with transactions for the current database

    """
    def __init__(self):
        self.database = Data()
        self.transaction_handler = TransactionHandler(self.database)

    def get(self, key):
        """Fetches the latest value of a key from the database

        Retrieves the value assigned for the given key.
        It varies based on the current status of the database:
        If a transaction is opened, it requests the latest value of the key to the
        transaction handler, in case of not being found, it lastly fetches it from the 
        database
        If there is no transaction opened, it fetches the value from the database

        Running Time: O(1)
        Both scenarios (transaction opened or not) have an amortized constant time
        due to each of the operations rely on dictionaries (hash tables) to store
        and retrieve data.
        All operations involved within the transaction handler when a transaction is opened
        run as well in amortized constant time

        Args:
            key: an string representing the key to fetch

        Returns:
            an string representing the correspondant value stored for the given key
            if the value is not found, it returns None

        """
        if self.is_transaction_active():
            value, found = self.transaction_handler.get(key)
            if found: 
                return value
        return self.database.data.get(key, None)

    def set(self, key, new_value):
        """Assigns a new value for the given key

        Assigns a value for the given key.
        It varies based on the current status of the database:
        If a transaction is opened, it transfers the operation to the transaction handler and
        sets the new value for this key in the transaction data storage, without touching the
        database.
        If there is no transaction opened, it modifies directly into the database
        It also updates the frequency of each value within the database, or transaction data if a 
        transaction is opened

        Assuming there should not be changes if the current value is the same as the new value,
        this method does nothing when this scenario occurs

        Running Time: O(1)
        Both scenarios (transaction opened or not) have an amortized constant time
        due to each of the operations rely on dictionaries (hash tables) to store
        and retrieve data.
        All operations involved within the transaction handler when a transaction is opened
        run as well in amortized constant time


        Args:
            key: an string representing the key to modify
            new_value: an string representing the new value for the given key
        """
        old_value = self.get(key)       
        if old_value != new_value:
            if self.is_transaction_active():
                self.transaction_handler.set(key, old_value, new_value)
            else:
                self.database.data[key] = new_value
                Data.decrease_freq(self.database.values_freq, old_value)
                Data.increase_freq(self.database.values_freq, new_value)
            
    def unset(self, key):
        """Removes the key from the database, like it was never set

        Eliminates the key along with its value from the database
        It varies based on the current status of the database:
        If a transaction is opened, it transfers the operation to the transaction handler, which
        adds a record that represents the deletion of this key
        If there is no transaction opened, it removes it directly from the database
        It also updates the frequency of each value within the database, or transaction data if a 
        transaction is opened

        Running Time: O(1)
        Both scenarios (transaction opened or not) have an amortized constant time
        due to each of the operations rely on dictionaries (hash tables) to store
        and retrieve data.
        All operations involved within the transaction handler when a transaction is opened
        run as well in amortized constant time

        Args:
            key: an string representing the key to remove
        """
        old_value = self.get(key)
        if self.is_transaction_active():
            self.transaction_handler.unset(key, old_value)
        else:
            self.data.pop(key, None)
            Data.decrease_freq(self.database.values_freq, old_value)

    def num_equal_to(self, value):
        """Retrieves the number of keys (variables) currently set to 'value'

        Retrieves the total frequency of keys that share the same value in the database
        and transaction together
        Regardless of a transaction being opened or not, it returns the frequency from the 
        current database aggregated with the frequencies modified by the transaction handler

        Running Time: O(1)
        Both scenarios (transaction opened or not) have an amortized constant time
        due to each of the operations rely on dictionaries (hash tables) to store
        and retrieve data.
        All operations involved within the transaction handler when a transaction is opened
        run as well in amortized constant time

        Args:
            value: an string representing the value to request and obtain the number of times this
                   value is assigned

        Returns:
            an integer, representing the total frequency of this value
        """
        return self.database.values_freq.get(value, 0) + self.transaction_handler.num_equal_to(value)

    def begin(self):
        """Opens a transaction

        This method opens a new transaction block. Each of this transactions can be 'nested',
        in other words, there can be transactions opened one after another, 
        having more relevance the transactions lastly created.

        Running Time: O(1)
        This operation is transfered to the transaction handler which opens a new 
        transaction in constant time
        """
        self.transaction_handler.begin()

    def rollback(self):
        """Undo all operations from the most recent transaction

        This method closes an opened transaction block. Particularly, it closes the most recent 
        transaction and undo all changes made by it.

        Running Time: O(1 + m)
        Being 'm' a variable that represents the number of keys modified in the latest
        transaction
        If there are no transactions opened, it runs in constant time.

        Returns:
            a boolean, representing the execution or not of the operation.
        """
        if self.is_transaction_active():
            self.transaction_handler.rollback()
            return True
        else:
            return False

    def commit(self):
        """Applies all the changes made by the transactions

        It applies all the changes from the most recent transaction to the oldest, having more
        relevance changes from recent transactions than olders.
        All these changes are set into the database modifying its values and frequencies.

        Running Time: O(m)
        Being 'm' a varable that represents the number of keys modified globally by all
        transactions
        If there are no transactions opened, it runs in constant time.

        Returns:
            a boolean, representing the execution or not of the operation.
        """
        if self.is_transaction_active():
            self.transaction_handler.commit()
            return True
        else:
            return False

    def is_transaction_active(self):
        """Returns the existance of a transaction

        Running Time: O(1)        
        Returns:
            a boolean, representing the existance of a transaction
        """
        return self.transaction_handler.is_active()




class TransactionHandler(object):
    """Handler that manages all transaction operations

    This class performs and controls all operations for the transactions. It handles
    a particular database for which these operations will be commited when desired.

    Args:
        database: an object of type Database representing an existent database.
                  its required for commiting changes and other few operations, such as
                  retrieving the latest value of a key from transactions or database.

    Attributes:
        database: an object of type Database representing an existent database.
                  its required for commiting changes and other few operations, such as
                  retrieving the latest value of a key from transactions or database.

        transactions: an object of type Data that stores all changes made by the
                      handler. Particularly, it represents a key-value list structure.
                      Example:
                        {
                            'a': ['1', '2', '3']
                            'z': ['8', None]
                        }
                        {
                            '3': 1
                            '8': -1
                        }
                        Being '3' the latest value for the key 'a' and 1 the global
                        frequency of this value within the transaction handler
                        considering the frequencies in the database

        transactions_opened: a list (or stack) of transactions
                             each transaction is represented as a 'set' that contains all
                             keys 'touched' within it. Since a set is a structure that does not 
                             store repeated values, it uniquely stores each key once, no matter
                             all the changes made to a key within the same transaction

                             Example:
                             [
                                set: ['a', 'z']
                             ]

    """
    def __init__(self, database):
        self.database = database

        self.transactions = Data()
        self.transactions_opened = []

    def get(self, key):
        """Fetches the latest value of a key from the transaction data

        Running Time: O(1)
        It runs in amortized constant time due to searching the key in a hash table
        and lastly retrieving the value from the last index of the array.

        Args:
            key: an string representing the key to fetch

        Returns:
            a tuple, containing a string that represents the correspondant 
            value stored for the given key and a boolean that representes the existance
            of the given key in the transaction data

            both are required since the latest value stored in this structure can be 'None'
            in the case it has been unset. Therefore, its required to consistently
            have knowledge of the real existence or not of the key
        """
        key_list = self.transactions.data.get(key, [])
        if key_list: 
            latest_value = key_list[-1]
            return (latest_value, True)
        return (None, False)

    def set(self, key, old_value, new_value):
        """Assigns a new value for the given key within the transaction data

        Assigns a value for the given key.
        It records this operation in the most recent transaction by adding the key into the set, 
        updates the frequencies, and adds if required the new value within the value list 
        of this key
        It also updates the frequency of each value within the transaction data considering
        the database data by receiving from it the old value of this key.

        Running Time: O(1)
        This method run in amortized constant time. It searches and modifies the hash table
        representing the transaction data and values frequency in average O(1)

        Args:
            key: an string representing the key to modify
            old_value: an string representing the previous value for the given key
            new_value: an string representing the new value for the given key
        """
        if self.is_active():
            latest_transaction = self.transactions_opened[-1]
            
            if key not in self.transactions.data: 
                self.transactions.data[key] = []

            key_list = self.transactions.data[key]
            if key in latest_transaction:
                key_list[-1] = new_value
            else:
                key_list.append(new_value)
                latest_transaction.add(key)

            Data.decrease_freq(self.transactions.values_freq, old_value)
            Data.increase_freq(self.transactions.values_freq, new_value)

    def unset(self, key, old_value):
        """Records the removal of the key in the transaction

        Verifies the existance of the key in the most recent transaction and
        adds this operation to the value list of the key if required, if not it updates it
        with 'None'
        If the current value of this key is already set to None, it does nothing
        It also updates the frequency of each value within the transaction data considering
        the database data by receiving from it the old value of this key.

        Running Time: O(1)
        This method run in amortized constant time. It searches and modifies the hash table
        representing the transaction data and values frequency in average O(1)
        If the old value is already None it does nothing, therefore it performs O(1) as well

        Args:
            key: an string representing the key to remove
        """
        if self.is_active() and old_value is not None:
            latest_transaction = self.transactions_opened[-1]
            self.transactions.data[key] =  self.transactions.data.get(key, [])          
            key_list = self.transactions.data[key]

            if key in latest_transaction:
                key_list[-1] = None
            else:
                latest_transaction.add(key)
                key_list.append(None)

            Data.decrease_freq(self.transactions.values_freq, old_value)

    def num_equal_to(self, value):
        """Retrieves the number of keys (variables) currently set to 'value'

        Retrieves the total frequency of keys that share the same value in the transaction data
        If the frequencies of a value are different from the database, this method may
        return a negative number representing the difference.

        Running Time: O(1)
        This method run in amortized constant time. It searches in the hash table
        that represents the frequence of the values from the globally from the transactions

        Args:
            value: an string representing the value to request and obtain the number of times this
                   value is assigned

        Returns:
            an integer, representing the total frequency of this value
        """
        return self.transactions.values_freq.get(value, 0)

    def begin(self):
        """Opens a transaction

        This method opens a new transaction block. Each of this transactions can be 'nested',
        in other words, there can be transactions opened one after another, 
        having more relevance the transactions lastly created.

        Running Time: O(1)
        """
        self.transactions_opened.append(set([]))

    def rollback(self):
        """Undo all operations from the most recent transaction

        This method closes an opened transaction block. Particularly, the most recent 
        transaction and undo all changes made by it.

        Running Time: O(m)
        Being 'm' a variable that represents the number of keys modified in the latest
        transaction
        """
        if self.is_active():
            if self.is_one_active():
                self.clear()                
            else:
                latest_transaction = self.transactions_opened.pop()             
                for modified_key in latest_transaction:
                    key_list = self.transactions.data[modified_key]

                    modified_value = key_list.pop()
                    previous_value, found = self.get(modified_key)
                    if not found: 
                        previous_value = self.database.data.get(modified_key, None)

                    Data.increase_freq(self.transactions.values_freq, previous_value)
                    Data.decrease_freq(self.transactions.values_freq, modified_value)

                    if not key_list: 
                        self.transactions.data.pop(modified_key, None)

    def commit(self):
        """Applies all the changes made by the transactions

        It applies all the changes from the most recent transaction to the oldest, having more
        relevance changes from recent transactions than olders.
        All these changes are set into the database modifying its values and frequencies.
        Lastly, it clears the transaction handler data

        Running Time: O(m)
        Being 'm' a varable that represents the number of keys modified globally by all
        transactions
        """
        if self.is_active():
            for key, key_list in self.transactions.data.items():
                value = key_list.pop()              
                if value:
                    self.database.data[key] = value
                else:
                    self.database.data.pop(key, None)

            for value, freq in self.transactions.values_freq.items():
                Data.modify_freq(self.database.values_freq, value, freq)
            
            self.clear()

    def clear(self):
        """Clears all transaction data, value frequencies and open transactions"""
        self.transactions = Data()
        self.transactions_opened = []

    def is_active(self):
        """Returns the existance of an opened transaction"""
        return (self.get_active_size() > 0)

    def is_one_active(self):
        """Returns the existance of a single opened transaction"""
        return (self.get_active_size() == 1)

    def get_active_size(self):
        """Returns the number of opened transactions"""
        return (len(self.transactions_opened))



class Data(object):
    def __init__(self):
        self.data = {}
        self.values_freq = {}

    @staticmethod
    def increase_freq(values_freq, key_of_value):
        """Increases the frequency a value is present in the data

        Running Time: O(1)
        """
        Data.modify_freq(values_freq, key_of_value, 1)

    @staticmethod
    def decrease_freq(values_freq, key_of_value):
        """Decreases the frequency a value is present in the data

        Running Time: O(1)
        """
        Data.modify_freq(values_freq, key_of_value, -1)
        
    @staticmethod
    def modify_freq(values_freq, key_of_value, num):
        """Modifies the frequency a value is present in the data by 'num' times
        If the new value frequence is equal to 0, the record is removed in order to save memory

        Running Time: O(1)
        """
        if key_of_value is not None:
            values_freq[key_of_value] = values_freq.get(key_of_value, 0) + num

            if values_freq[key_of_value] == 0:
                values_freq.pop(key_of_value, None)



class DBConsole(object):
    """Handler that manages all console operations

    This class performs and controls all console operations for the simple database.

    Attributes:
        database: an object of type Database representing an existent database.

        end_operation: a set of strings representing all methods that may finish the execution
                       of the process

        valid_operations_arguments: a set of tuples representing all valid operations and the
                                    number of arguments required for them to be valid
    """
    def __init__(self):
        self.database = Database()
        self.end_operation = set(['END'])
        self.valid_operations_arguments = set([
            ('GET',         1),
            ('SET',         2),
            ('UNSET',       1),
            ('NUMEQUALTO',  1),
            ('BEGIN',       0),
            ('ROLLBACK',    0),
            ('COMMIT',      0)
        ])

    def read_from_stdin(self):
        """Reads from stdin and executes the specified command

        Reads from input stdin and parses each line command. It compares its content 
        with the valid operations and if so executes it.
        Moreover, if input comes from a file, it handles 'end of file' events and completes
        the execution 

        Avoided using getattr for function calls due to performance is reduced as per python documentation.
        """
        try: 
            fields = input().split()

            if fields:
                method_name = fields[0].upper()
                arguments = fields[1:]
                arguments_count = len(arguments)

                if method_name in self.end_operation: 
                    return False


                if (method_name, arguments_count) in self.valid_operations_arguments:

                    if method_name == 'GET': 
                        output = self.database.get(*arguments)
                        if not output:  output = 'NULL'
                        print (output)

                    elif method_name == 'SET':
                        self.database.set(*arguments)

                    elif method_name == 'UNSET':
                        self.database.unset(*arguments)

                    elif method_name == 'NUMEQUALTO':
                        output = self.database.num_equal_to(*arguments)
                        print (output)


                    elif method_name == 'BEGIN':
                        self.database.begin()

                    elif method_name == 'ROLLBACK':
                        output = self.database.rollback()
                        if not output:
                            print ("NO TRANSACTION")

                    elif method_name == 'COMMIT':
                        output = self.database.commit()
                        if not output:
                            print ("NO TRANSACTION")
                else:
                    print ('Invalid method or number of arguments')
            return True
        except EOFError:
            return False

    def listen(self):
        """Consistently listens for commands

        This method stays running until a value from the end of operation set is 
        sent from stdin.        
        """
        active = True       
        while active:
            active = self.read_from_stdin()
            
def main():
    DBConsole().listen()

if __name__ == "__main__":
    main()