package com.hbase.learn.hbase_action.ch04;

import java.io.IOException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.ByteStringer;

import com.google.protobuf.InvalidProtocolBufferException;



public class CustomFilter extends FilterBase {

	private byte[] value = null;
	private boolean filterRow = true;

	public CustomFilter() {
		super();
	}

	public CustomFilter(byte[] value) {
		this.value = value;
	}

	@Override
	public boolean filterRowKey(byte[] buffer, int offset, int length) throws IOException {
		// TODO Auto-generated method stub
		filterRow = false;
		return filterRow;
	}

	@Override
	public ReturnCode filterKeyValue(Cell v) throws IOException {
		// TODO Auto-generated method stub
		if (CellUtil.matchingValue(v, value)) {
			filterRow = false;
		}

		return ReturnCode.INCLUDE;
	};

	@Override
	public boolean filterRow() throws IOException {
		// TODO Auto-generated method stub
		return filterRow;
	}

	@Override
	public void reset() throws IOException {
		// TODO Auto-generated method stub
		super.reset();
		
		this.filterRow = true;
	}

	@Override
	public boolean filterAllRemaining() throws IOException {
		// TODO Auto-generated method stub
		return super.filterAllRemaining();
	}


	@Override
	public byte[] toByteArray() {
		FilterProtos.CustomFilter.Builder builder = FilterProtos.CustomFilter.newBuilder();
		if (value != null)
			builder.setValue(ByteStringer.wrap(value)); // co CustomFilter-6-Write Writes the given value out so it can
														// be sent to the servers.
		return builder.build().toByteArray();
	}

	// @Override
	public static Filter parseFrom(final byte[] pbBytes) throws DeserializationException {
		FilterProtos.CustomFilter proto;
		try {
			proto = FilterProtos.CustomFilter.parseFrom(pbBytes); // co CustomFilter-7-Read Used by the servers to
																	// establish the filter instance with the correct
																	// values.
		} catch (InvalidProtocolBufferException e) {
			throw new DeserializationException(e);
		}
		return new CustomFilter(proto.getValue().toByteArray());
	}
	
	
}
