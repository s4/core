/*
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 	        http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific
 * language governing permissions and limitations under the
 * License. See accompanying LICENSE file. 
 */
package io.s4.util;

import java.math.BigInteger;

public class NumberUtils {

    private final static BigInteger B64 = BigInteger.ZERO.setBit(64);

    public static BigInteger getLongAsUnsignedBigInteger(long number) {
        if (number >= 0)
            return BigInteger.valueOf(number);
        return BigInteger.valueOf(number).add(B64);
    }

    public static String getUnsignedBigIntegerAsHex(BigInteger bi) {
        String hexString = bi.toString(16);
        if (hexString.length() < 16) {
            String zeroes = "000000000000000";
            hexString = zeroes.substring(0, 16 - hexString.length())
                    + hexString;
        }
        return hexString.toUpperCase();
    }

    public static void main(String args[]) {
        BigInteger bi = getLongAsUnsignedBigInteger(0x00f1200000004561L);
        System.out.println(getUnsignedBigIntegerAsHex(bi));

        bi = getLongAsUnsignedBigInteger(0x80f12000dd004561L);
        System.out.println(getUnsignedBigIntegerAsHex(bi));

        bi = getLongAsUnsignedBigInteger(0x1161L);
        System.out.println(getUnsignedBigIntegerAsHex(bi));
    }
}
