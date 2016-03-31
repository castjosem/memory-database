class Database(object):
    def __init__(self):
        self.database = Data()
        self.transaction_handler = TransactionHandler(self.database)

    def get(self, key):
        if self.is_transaction_active():
            value, found = self.transaction_handler.get(key)
            if found: 
                return value
        return self.database.data.get(key, None)

    def set(self, key, new_value):
        old_value = self.get(key)       
        if old_value != new_value:
            if self.is_transaction_active():
                self.transaction_handler.set(key, old_value, new_value)
            else:
                self.database.data[key] = new_value
                Data.decrease_freq(self.database.values_freq, old_value)
                Data.increase_freq(self.database.values_freq, new_value)
            
    def unset(self, key):
        old_value = self.get(key)
        if self.is_transaction_active():
            self.transaction_handler.unset(key, old_value)
        else:
            self.data.pop(key, None)
            Data.decrease_freq(self.database.values_freq, old_value)

    def num_equal_to(self, value):
        return self.database.values_freq.get(value, 0) + self.transaction_handler.num_equal_to(value)

    def begin(self):
        self.transaction_handler.begin()

    def rollback(self):
        if self.is_transaction_active():
            self.transaction_handler.rollback()
            return True
        else:
            return False

    def commit(self):
        if self.is_transaction_active():
            self.transaction_handler.commit()
            return True
        else:
            return False

    def is_transaction_active(self):
        return self.transaction_handler.is_active()


class TransactionHandler(object):
    def __init__(self, database):
        self.database = database

        self.transactions = Data()
        self.transactions_opened = []

    def get(self, key):
        key_list = self.transactions.data.get(key, [])
        if key_list: 
            latest_value = key_list[-1]
            return (latest_value, True)
        return (None, False)

    def set(self, key, old_value, new_value):
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
        return self.transactions.values_freq.get(value, 0)

    def begin(self):
        self.transactions_opened.append(set([]))

    def rollback(self):
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
        self.transactions = Data()
        self.transactions_opened = []

    def is_active(self):
        return (self.get_active_size() > 0)

    def is_one_active(self):
        return (self.get_active_size() == 1)

    def get_active_size(self):
        return (len(self.transactions_opened))

class Data(object):
    def __init__(self):
        self.data = {}
        self.values_freq = {}

    @staticmethod
    def increase_freq(values_freq, key_of_value):
        Data.modify_freq(values_freq, key_of_value, 1)

    @staticmethod
    def decrease_freq(values_freq, key_of_value):
        Data.modify_freq(values_freq, key_of_value, -1)
        
    @staticmethod
    def modify_freq(values_freq, key_of_value, num):
        if key_of_value is not None:
            values_freq[key_of_value] = values_freq.get(key_of_value, 0) + num

            if values_freq[key_of_value] == 0:
                values_freq.pop(key_of_value, None)

class DBConsole(object):
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
        active = True       
        while active:
            active = self.read_from_stdin()
            
def main():
    DBConsole().listen()

if __name__ == "__main__":
    main()