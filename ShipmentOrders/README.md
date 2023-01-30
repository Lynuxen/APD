# APD - Tema 2: Manager de comenzi de Black Friday in Java

## Functionality

I use two two thread pools to manage the threads, one for each level. I submit
P amount of tasks to the first thread pool, meaning every "employee" has his or
hers tasks already predetermined (more on this in README_BONUS). The first
level reads an order, then submits `numberOfItems` tasks to the second thread
pool. Since I initialized both thread pools with 
`newFixedThreadPool(numberOfThreads)`, both levels will have a maximum of `P`
running threads at the same time. Each "employee" of the second level also has
his own chunk to solve, so they don't overlap with items to be shipped from
other threads, as mentioned in `Atentie!` in the enunt. Once every chunk of
order_products has been checked and the items have been shipped, the first
level sends out the shipment. Locks have been used to make sure threads don't
overlap each other when writing to the output files.