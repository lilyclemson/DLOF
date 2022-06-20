EXPORT File_dlof := MODULE
EXPORT Layout := RECORD
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
  END;

EXPORT File := DATASET('~class:ara:lof_datset::dlof_datset.csv',Layout,CSV);
END;