all:
	cp ../build/libpgeon.dylib .
	MACOSX_DEPLOYMENT_TARGET=12.0 python setup.py build_ext --inplace

clean:
	rm -fr build
	rm pgeon.cpp
	rm pgeon.cpython-*-darwin.so

test:
	DYLD_LIBRARY_PATH="${PWD}" python -c "import pgeon; t=pgeon.GetTable('postgresql://localhost/mytests', 'select * from numeric_table').to_pandas(); print(t)"
	DYLD_LIBRARY_PATH="${PWD}" python -c "import pgeon; t=pgeon.GetTable('postgresql://localhost/mytests', \"select (a_json->>'a')::int as u from json_table\").to_pandas(); print(t)"
	DYLD_LIBRARY_PATH="${PWD}" python -c "import pgeon; t=pgeon.GetTable('postgresql://localhost/mytests', 'select * from enum_table').to_pandas(); print(t)"
	DYLD_LIBRARY_PATH="${PWD}" python -c "import pgeon; t=pgeon.GetTable('postgresql://localhost/mytests', 'select * from minute_bars limit 10').to_pandas(); print(t)"
