package com.hbase.learn.hbase_action.ch04;

import java.io.IOException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;

import com.google.protobuf.InvalidProtocolBufferException;

public class CustomFilterL extends FilterBase {

	private String start_row;
	private String end_row;
	private boolean filterRow = true;

	public CustomFilterL() {
		super();
	}

	public CustomFilterL(String start_row, String end_row) {
		this.start_row = start_row;
		this.end_row = end_row;
	}

	@Override
	public boolean filterRowKey(byte[] buffer, int offset, int length) throws IOException {
		// TODO Auto-generated method stub
		filterRow = false;
		return filterRow;
	}

	@Override
	public ReturnCode filterKeyValue(Cell v) throws IOException {

		if (new String(v.getRow()).compareToIgnoreCase(start_row) >= 0
				&& new String(v.getRow()).compareToIgnoreCase(end_row) <= 0) {
			return ReturnCode.INCLUDE;
		}

		   return ReturnCode.NEXT_ROW;
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
		FilterProtosL.CustomFilterL.Builder builder = FilterProtosL.CustomFilterL.newBuilder();
		if (start_row != null)
			builder.setStartRow(start_row);

		if (end_row != null)
			builder.setEndRow(end_row);

		return builder.build().toByteArray();
	}

	// @Override
	public static Filter parseFrom(final byte[] pbBytes) throws DeserializationException {
		FilterProtosL.CustomFilterL proto;
		try {
			proto = FilterProtosL.CustomFilterL.parseFrom(pbBytes);
		} catch (InvalidProtocolBufferException e) {
			throw new DeserializationException(e);
		}
		return new CustomFilterL(
				proto.getStartRow(), proto.getEndRow()
//				Bytes.toString(proto.getStartRowBytes().toByteArray()),
//				Bytes.toString(proto.getEndRowBytes().toByteArray())				
				);
	}

}
