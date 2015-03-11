#reads a flat file and assigns a unique docname to every n lines, writes to stdout

def assign_docs(inputFile, n):
    docName = 'doc1';
    docNumber = 1;
    docLines = 0;

    for line in open(inputFile, 'r'):
        docLines += 1;
        print '%s\t%s' % (docName, line),
        if(docLines == n):
            docNumber += 1;
            docName = 'doc' + str(docNumber);
            docLines = 0;




if __name__ == '__main__':
    import sys
    n = int(sys.argv[2]);
    if(n == 0):
        print 'n has to be positive'
        sys.exit(0)
    inputFile = sys.argv[1];
    assign_docs(inputFile, n)


