# README #

This repo is the demo transaction csv handler.

## Several ideas ##

### Performance ###

csv file maybe have a lot additional spaces. So I use `csv_parser/clean` to clean all space inside. Function `handle_transaction_file` reads whole file to the string, so when the file is huge. It might be cause some problem. 

**Ideas**

> maybe read file line by line, make it like a stream. Except the header, each line is a record, which can used inside `src/client`. Save memory.
>
> maybe use BufReader for csv file? Maybe not, because BufReader used in the situation `small and repeated read calls to the same file`. 
>
> but, if we have a network socket (tcp connection) of our csv data, we can use it (I guess).

**update on 5/27/2022**

`test_read_stream` in `src/csv_parser.rs` use `BufReader` run the `TcpStream` for reading each line of csv tcp stream (without headers). So we can use stream rather than read a big file, beside, stream reading can fit concurrency easily.
