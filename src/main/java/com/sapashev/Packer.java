package com.sapashev;

/**
 * Contains method for packing/unpacking int and long values to the one compound long value;
 * minor - number of less significant bits to store length of line (measured in chars).
 * major - number of more significant bits to store position of line start (measured in bytes).
 * @author Arslan Sapashev
 * @since 28.12.2016
 * @version 1.0
 */
public class Packer {
    private final int minor;
    private final int major;

    public Packer(int minor, int major){
        if(minor > 32 || major <= 32){
            this.minor = 26;
            this.major = 37;
        } else {
            this.minor = minor;
            this.major = major;
        }

    }

    /**
     * Packs int and long values to the one long value.
     * Length of line - stored in less significant n bits of resulting long value.
     * Start of line  - stored in more significant n bits of resulting long value.
     * Most significant bit is preserved for sign.
     * @param length - length of line
     * @param position - number of line start in the file (in bytes)
     * @return packed long value;
     */
    public long packToLong(int length, long position){
        if((length < (1 << minor)) && (position < (1L << major))){
            long l = position;
            return ((l << minor) | length);
        } else {
            throw new RuntimeException("Too big length or position");
        }
    }

    /**
     * Unpacks and retrieves position of line start from the compounded long value;
     * @param l - compounded long value;
     * @return - ordinal number of byte from which particular line starts.
     */
    public long getPosition (long l){
        return ((l >>> minor)|0);
    }

    /**
     * Unpacks and retrieves line length from the compound long value;
     * @param l - compound long value.
     * @return - length of particular line estimated as count of characters.
     */
    public int getLength (long l){
        int i = (int)(l | 0);
        i <<= (32 - minor);
        i >>>= (32 - minor);
        return i;
    }
}
