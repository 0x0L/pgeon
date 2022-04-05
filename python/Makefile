all:
	MACOSX_DEPLOYMENT_TARGET=12.0 python setup.py build_ext --inplace
	cp ../build/liblibpgeon.dylib .

clean:
	rm -fr build
	rm pgeon.cpp
	rm pgeon.cpython-39-darwin.so

test:
	DYLD_LIBRARY_PATH="${PWD}" ipython -c "import pgeon; pgeon.Getoo(b'postgresql://localhost/mytests', b'select * from numeric').to_pandas()"
