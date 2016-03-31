#Simple Database Challenge

To run the program execute the following command in CMD:

    > python simple_database.py
The program will start listening for commands written in the console.

    > python simple_database.py < filename.txt
The program will read commands from the file given


###Available commands
    
+ SET name value:
    Sets the variable name to the value value. Neither variable names nor values should contain spaces.

+ GET name
    Prints out the value of the variable name, or NULL if that variable is not set.

+ UNSET name
    Unsets the variable name, making it just like that variable was never set.

+ NUMEQUALTO value
    Prints out the number of variables that are currently set to value. If no variables equal that value, prints 0.

+ BEGIN
    Opens a new transaction block. Transaction blocks can be nested; a BEGIN can be issued inside of an existing block.

+ ROLLBACK
    Undo all of the commands issued in the most recent transaction block, and close the block. 
    Print nothing if successful, or print NO TRANSACTION if no transaction is in progress.

+ COMMIT
    Close all open transaction blocks, permanently applying the changes made in them. 
    Print nothing if successful, or print NO TRANSACTION if no transaction is in progress.
+ END
    Exits the program.
