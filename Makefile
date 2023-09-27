gen-api: gen-curp-api gen-xline-api

gen-curp-api:
	rm -rf api/curp && \
	mkdir -p api/curp && \
	cd curp-proto/src && \
	python3 -m grpc_tools.protoc \
		-I. \
		--python_out=../../api/curp \
		--pyi_out=../../api/curp \
		--grpc_python_out=../../api/curp \
	*.proto

gen-xline-api:
	rm -rf api/xline && \
	mkdir -p api/xline && \
	cd xline-proto/src && \
	python3 -m grpc_tools.protoc \
		-I. \
		--python_out=../../api/xline \
		--pyi_out=../../api/xline \
		--grpc_python_out=../../api/xline \
	*.proto
