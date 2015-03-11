def assignsentid(inputfile):
    line_number = 1
    for line in open(inputfile):
        print '%d\t%s' % (line_number, line),
        line_number += 1

if __name__ == '__main__':
    import sys
    assignsentid(sys.argv[1])

