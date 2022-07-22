EXPORT File_test := MODULE
EXPORT Lay := RECORD
    REAL4  field1;
    REAL4  field2;
    REAL4  field3;
    REAL4  field4;
    REAL4  field5;
    REAL4  field6;
    REAL4  field7;
    REAL4  field8;
    REAL4  field9;
    REAL4  field10;
    REAL4  field11;
    REAL4  field12;
    REAL4  field13;
    REAL4  field14;
    REAL4  field15;
    REAL4  field16;
    REAL4  field17;
    REAL4  field18;
    REAL4  field19;
    REAL4  field20;
    REAL4  field21;
    REAL4  field22;
    REAL4  field23;
    REAL4  field24;
    REAL4  field25;
    REAL4  field26;
    REAL4  field27;
    REAL4  field28;
    REAL4  field29;
    REAL4  field30;
   
  END;

// EXPORT File_test1 := DATASET('~class::ara::test::creditcard1.csv',Lay,CSV);

EXPORT File_test1 := DATASET('~class::ara::lof_no_dup::credit_no_duplicate.csv',Lay,CSV);
END;
