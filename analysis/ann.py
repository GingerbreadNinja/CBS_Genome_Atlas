import re
import argparse

feature_regex = re.compile( r"^FEATURE\s*" )
annotation_regex = re.compile( r"^\s{5}(?P<feat>[a-z_A-Z]+)\s*(?P<rest>\S*)\s*" )
origin_regex = re.compile( r"^ORIGIN\s*" )
position_regex = re.compile( r"^(?P<comp>\D*)(?P<start>\d+)(\.)*(?P<end>\d*).*" )
gene_regex = re.compile( r"^.*/gene\W*\"(?P<gene>\w+)\".*" )

# CDS              4632464  4633333 - rob
out_format = "{:<15s}{:>9s}{:>9s} {:s} {:s}"

def make_ann(in_file, out_file):
    # Declarations
    hit_features = False
    in_feature = False
    
    f = None
    s = None
    d = None
    e = None
    l = None
    
    # Line by line parse the file...
    for line in in_file:
        if not hit_features:
            if feature_regex.match( line ):
                hit_features = True
            continue
        ann = annotation_regex.match( line )
        if ann:
            if in_feature:
                # DUMP THE OLD FEATURE
                print >> out_file, out_format.format(f,s,e,d,l)

            in_feature = True
            f = ann.group('feat')
            l = f
            s = None
            d = None
            e = None

            pos = position_regex.match( ann.group('rest') )
            if pos:
                s = pos.group('start')
                d = '+'
                e = pos.group('end')
                if pos.group('comp'):
                    d = '-'
                if not e:
                    e = s
            continue
        loc = gene_regex.match( line )
        if loc:
            l = loc.group('gene')
        elif origin_regex.match( line ):
            if in_feature: # DUMP LAST FEATURE
                print >> out_file, out_format.format(f,s,e,d,l)
            return
        else:
            continue
    return

def main():
    parser = argparse.ArgumentParser(description="Extract annotations from a GBK file")   
    parser.add_argument('input', type=argparse.FileType('r'), help='The input GBK file')
    parser.add_argument('output', type=argparse.FileType('w'), help='The output ANN file')
    args = parser.parse_args()
    make_ann( args.input, args.output )
    args.input.close()
    args.output.close()

if __name__ == '__main__':
    main()

