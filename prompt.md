# Version 1

## Zerodha socket connector
1. Write a detailed python script that log in on a daily basis at 8:45 AM using the stored zerodha credentials, and subscribe to tick data of the symbol that is given as an input while starting the script.
2. For example if the input symbol is NIFTY, get all weekly and monthly expiry symbol from the instrument list and subscribe to it. 
3. On market start, the script should receive the tick data from the socket and send it to a redis stream that is puts the tick in the queue for further processing. This is essential because the we cannot perform CPU intensive tasks inside socket that will block the pipeline from recording the data.
4. One can open three socket with one API_KEY in zerodha, therefore efficiently subscribe to the symbols in the socket for maximum throughput.
5. Provide a config file to input all the credentials for log-in.

## Redis receiver
1. On receiving the ticks dump from the zerodha socket. The receiver has to segregate the data based on the symbol and time and store in the redis db. 
2. The data received from zerodha shall have depths either of level 5 or 20 for some instruments. The DB should be structured in such a way that, all essential fields of the received tick data should be saved for future analysis.
3. Periodically save the redis data in a persistent memory to preserve the data. This persistent save should not be in RAM.
4. This receiver should not block the socket at any cost.


