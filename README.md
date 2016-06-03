# A Recoverable Virtual Memory based on the idea of RioVista

Have implemented a variation of Rio Vista here, as there is no battery backed supported file cache.  

Persistency is accomplished by writing to the disk during commits. Whenever the user commits a transaction, the corresponding mapped memory is persisted to the disk. Hence, user won't lose any information, if crashed after a commit operation.  

