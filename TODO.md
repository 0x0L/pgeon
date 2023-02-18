# TODO

- Tests & benchmarks

- Error handling: replace the hideous and error prone `unpack(buf); buf += ...` with a Buffer struct; that would allow to return `Status` instead of the read len

- Multi platform build / package / deploy

- Batchbuilder simple `void (*callback)(std::shared_ptr<arrow::RecordBatch>)` interface

- Standalone Flight server

- Is there any issue with `COPY` ? If so, explore use of `FETCH` again

- Output format for bit(..)

- Properly integrate UserOptions

  - control which strings (or columns) should be dict encoded. maybe as a default char varchar should be dict encoded and text should be large_utf8

  - review of bytes vs string and encoding

- python bindings

  - propagate user_options

  - proper package / docstring / etc...
