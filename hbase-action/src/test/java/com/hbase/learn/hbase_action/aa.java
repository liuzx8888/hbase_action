package com.hbase.learn.hbase_action;

import java.io.IOException;
import java.io.Reader;

import org.antlr.runtime.CharStream;
import org.apache.phoenix.shaded.org.antlr.runtime.ANTLRReaderStream;

public class aa extends ANTLRReaderStream {
//	
	aa (Reader script) throws IOException {
         super(script);
     }
	@Override
	public int LA(int i) {
//        if (i == 0) { return 0; // undefined
//        }
//        if (i < 0) {
//            i++; // e.g., translate LA(-1) to use offset 0
//        }
//
//        if ((p + i - 1) >= n) { return CharStream.EOF; }
//        return Character.toLowerCase(data[p + i - 1]);
	
		if ( i==0 ) {
			return 0; // undefined
		}
		if ( i<0 ) {
			i++; // e.g., translate LA(-1) to use offset i=0; then data[p+0-1]
			if ( (p+i-1) < 0 ) {
				return CharStream.EOF; // invalid; no char before first char
			}
		}

		if ( (p+i-1) >= n ) {
            //System.out.println("char LA("+i+")=EOF; p="+p);
            return CharStream.EOF;
        }
        //System.out.println("char LA("+i+")="+(char)data[p+i-1]+"; p="+p);
		//System.out.println("LA("+i+"); p="+p+" n="+n+" data.length="+data.length);
		return data[p+i-1];
	
	}
	
	public static void main(String[] args) {
		

		
		
	}

}
