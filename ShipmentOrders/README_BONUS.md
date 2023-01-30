# Dealing with the chunks

I implemented the bonus for both of the files, and it pretty much works the
same. I get the file size with `file.length()`, divide it by the number of
threads for the first level, number of items for the second level. I get an
`approximateChunk`, which I then roll back the "pointer" to the previous line,
so I don't read in the middle of a line. As such, work is split pretty much
evenly, with the exception of the last thread picking up the remaining work.
I did do a slight correction for the first level at
` while (approximateChunk < 17)`, where in this case, the number of threads is
bigger than the number of orders to be processed.