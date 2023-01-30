# Tema 1 - Procesare paralela folosind paradigma Map-Reduce

## Initializing the homework

The homework was implemented in C++.

When the program starts, it opens the file passed as a parameter and reads
every input file listed and saves them in `std::vector<std::string> files`.

I use a struct `arguments` for passing arguments to the threads. Aside from the
id, number of mappers and reducers, it has pointers to a common mutex, barrier,
and a pointer to the files read from the `test.txt` file. It also has a pointer
to a vector of `struct mapper`, so that every thread can have access to every
mapper. This struct has a vector of vectors of ints. The position of each
vector that contains a vector of ints represents of what power a number is
(ex. 81 is at position 0 and at position 2, meaning its a square and an n^4).


## Thread description

When the threads are started, I have them split into mappers and reducers by
checking if `data->id < data->nmb_mappers` and `data->id >= data->nmb_mappers`.
Mappers have an `id` less than the number of mappers, while the reducers have a
an `id` bigger than the same number. Between the two if statements, I have a
barrier waiting for all threads to reach the same point
(`pthread_barrier_init(&barrier, NULL, nmb_threads)` line 199, initialized to
M + P). This is so no reducer starts working before the mappers have their part
finished. 


## Mappers

Mappers have their "files to be read" allocated dynamically. In an infinite
loop, the thread locks the mutex then reads if `data->files->size() == 0`.
If this is true, it means that there are no more files to be open and read and
the thread releases the lock, then breaks the loop.

The mapper gets a file from the back of the `data->files`, deletes it, and
begins reading it. For every number read in a file, it runs the function
`perfectPower(int arr[], int number, int power)` in a for loop that starts at
2 and ends at `data->nmb_reducers + 2` - 1, (see Algorithm below). If the
number is a perfect power, adds it at `i-2` (see Initializing the homework).
After this, I set `cur_mapper.did_work` to true. When the loop is finished, I
check to see if the current mapper did any work, and if it did, add it to
`data->mappers`. I did this because if the amount of work to be done is small,
and we have too many mappers, one mapper may do all the work, leaving
the other mappers empty.


## Reducers

Pretty simple here. For every reducer I have a set where I add every number
from a specific location (`numbers.at(data->id - data->nmb_mappers)`) from
every mapper, meaning every reducer only has the perfect power it was assigned
to have. At the end I print the size of every reducer.

## Algorithm

The most difficult part of this homework was finding an algorithm that didn't
run more than a 120s on the given input. I tried to figure it out on my own
but couldn't think of a general solution. So I searched on the web and found
[this explanation](https://cs.stackexchange.com/questions/48033/how-does-one-find-out-whether-n-ab-for-some-integer-a-and-b/48060#48060).
So I did just that. From line 110 to line 115, I call `perfectPower()` for
2,3,4 etc. The function `perfectPower` then does a binarySearch on
`int possible[ARBITRARY_LIMIT]` and the number that was read. If a number in
that array raised to the power of i (2,3,4..) is the number we have read, that
means we have a perfect power. The array is simply every number from 1 to 2047.
When I first implemented this algorithm, I did not have the correct answer for
the number of squares, because the input numbers were in the billions and there
was not enough numbers in the `possible` array to check. Increasing the size of
the array added way too much runtime, so I used casting to int to solve this.
`(int)sqrt(number) * (int)sqrt(number) == number`, that means that the sqrt of
the number we have read is an integer. I only use this for 2, the rest are
checked with the algorithm mentioned above.