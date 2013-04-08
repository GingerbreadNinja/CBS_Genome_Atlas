import struct
import gzip
import argparse
import math
from collections import deque
from Bio import SeqIO

lanes = {'dnap0':('.ornstein.gz',1),
         'dnap1':('.travers.gz',1),
         'dnap2':('.blast.Direct.gz',1),
         'dnap3':('.blast.Inverted.gz',1),
         'dnap4':('.baseskews.col.gz',3),
         'dnap5':('.baseskews.col.gz',4)}

lanes_new = [
              ('dnap0','.ornstein.gz',0),
              ('dnap1','.travers.gz',0),
              ('dnap2','.blast.Direct.gz',0),
              ('dnap3','.blast.Inverted.gz',0),
              ('dnap4','.baseskews.col.gz',2),
              ('dnap5','.baseskews.col.gz',3),
]

min_aggregate_count = 200

def makebin0( reference, length ):
    files = [ gzip.open( reference + lanes_new[i][1], 'r' ) for i in range(len(lanes_new)) ]
    bin_file = open( reference + ".data0.bin", 'wb' )
    for _ in range( length ):
        for i in range(len(lanes_new)):
            l = files[i].readline()
            f = 0;
            if l:
                try:
                    f = float( l.split()[ lanes_new[i][2] ] )
                except Exception, e:
                    f = 0;
            bin_file.write(struct.pack('f',f ))
        
    for f in files:
        try:
            f.close()
        except Exception, e:
            continue
    try:
        bin_file.close()
    except Exception, e:
        pass
    return

def makebinN( reference, length, N ):
    if( N == 0 ):
        makebin0( reference, length )
        return
    input_bin = open( reference + ('.data%i.bin' % (N - 1) ), 'r' )
    output_bin = open( reference + ('.data%i.bin' % (N) ), 'w' )
    l = len( lanes_new )

    current_deques = [ deque([], 4) for _ in lanes_new ]
    for i in range( 3 ):
        for j in range( l ):
            f = struct.unpack('f', input_bin.read(4))
            current_deques[j].append( f[0] )
    
    for i in range( int(length / math.pow(2, N)) ):
        for j in range( l ):
            f = struct.unpack('f', input_bin.read(4))
            current_deques[j].append( f[0] )
            avg = sum( current_deques[j] ) / 4
            output_bin.write( struct.pack('f', avg) )
    output_bin.close()

def makeAggregateN( ref, length, N ):
    l = int(length / math.pow(2, N))
    l_lanes = len(lanes_new)
    # (COUNT, AVG, STDDEV, MIN, MAX)
    values = [ [ l, 0.0, 0.0, float('+inf'), float('-inf')] for _ in lanes_new ]
    input_file = open(ref + ('.data%i.bin' % N ), 'r') 
    for i in range( l ):
        for j in range( l_lanes ):
            temp_mean = values[j][1]
            f_tup = struct.unpack('f', input_file.read(4))
            f = f_tup[0]
            values[j][1] += ( f - temp_mean ) / float( i + 1 )
            values[j][2] += ( f - temp_mean ) * ( f - values[j][1] )
            values[j][3] = min( values[j][3], f )
            values[j][4] = max( values[j][4], f )
    input_file.close()
    for j in range( l_lanes ):
        values[j][2] = math.sqrt( values[j][2] / (l - 1) )
    return values    

def makeAggregates( ref, length, maxN ):
    output_file = open( ref + '.aggregates.tab', 'w' )
    values = [ makeAggregateN( ref, length, i) for i in range(maxN) ]
    field_names = [lane[0] for lane in lanes_new]
    field_string = '\t'.join(field_names)
    print >> output_file, "fields %s" % field_string
    for i in range(len( values )):
        for j in range(len( lanes_new )):
            print >> output_file, "%i\t%s\tcount\t%i" % (i, field_names[j], values[i][j][0])
            print >> output_file, "%i\t%s\tavg\t%0.6f" % (i, field_names[j], values[i][j][1])
            print >> output_file, "%i\t%s\tstddev\t%0.6f" % (i, field_names[j], values[i][j][2])
            print >> output_file, "%i\t%s\tmin\t%0.6f" % (i, field_names[j], values[i][j][3])
            print >> output_file, "%i\t%s\tmax\t%0.6f" % (i, field_names[j], values[i][j][4])
    output_file.close()

def main():
    parser = argparse.ArgumentParser(description="Convert an atlas results into files for use with gwBrowser.")
    parser.add_argument('ref',type=argparse.FileType('r'), help="The reference genome to examine")
    args = parser.parse_args()
    handle = args.ref
    record = SeqIO.read(handle, format='gb')
    l = len(record)
    n = math.ceil( math.log( l / 100, 2) )
    for i in range(int(n)):
        makebinN( args.ref.name, l, i )
    makeAggregates( args.ref.name, l, int(n) )
    args.ref.close()

if __name__ == '__main__':
    main()
