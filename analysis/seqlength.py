from Bio import SeqIO
import argparse

def main():
    parser = argparse.ArgumentParser(description="Calculate GBK sequence length")
    parser.add_argument('input', type=argparse.FileType('r'), help='The input GBK file')
    args = parser.parse_args()
    handle = args.input
    record = SeqIO.read(handle, format='gb')
    print len(record)
    handle.close()
    return 0

if __name__ == '__main__':
    main()
