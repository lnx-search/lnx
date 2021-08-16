# Transactions

Transactions are one of the most important things in lnx, they control what data is seen by
the reader and what data is actually fully processed and saved.

The core concept of the transactions work on the same basis of tradition database transactions
with a one major difference:

- There *should* only ever be **ONE** system interacting with a writer for a specific index at a time.
The system processes writes in single a queue, so if one system adds a document and another commits
then the document will be finalized and flushed to disk.</br> 
**It is highly recommended to wrap the writing endpoints in a lock**

- Transactions are implicit vs explicit meaning you dont declare a transaction
you simple call commit and rollback respectively which apply their relevant affects
on writes made **since the last commit** once something is committed it cannot be rolled back.

### Transaction Endpoints
Commits any recent changes since the last commit.
This will finalise any changes and flush to disk.
```
POST /indexes/:index_name/commit
```

Reverts any recent changes since the last commit.
```
POST /indexes/:index_name/rollback
```