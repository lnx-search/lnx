# Creating An Index
Creating an index is fairly simple, once made lnx will it as a persistent config so that you 
dont need to continuously keep redefining it.

### Gotcha Moments
Creating an index can create quite a few gotcha moments so its important to understand the behaviour
of the system before ramming your head into a wall wondering why things arent updating or being
destroyed.

- This system is append / delete only, to update an index it must be deleted and re-made, all 
data is lost in the process. This is because the index is schema-full and requires fields exist
if define (This may change in future releases.)
- Creating indexes are **VERY HEAVY** operations, not only are you acquiring a global lock across
all indexes stopping anything else completing operations but you are also spawning and creating
several threads and thread pools (more on this later.) They should not be made randomly during 
usage and should not be made via some user input *THIS WILL CAUSE SERIOUS ISSUES OTHERWISE*.
- Deleting indexes clear all data stored, this means any previously uploaded documents will be
removed.

