setnums:= record
  integer num;
  end;
Layout_Person := RECORD
    UNSIGNED1 PersonID;
    STRING15 FirstName;
    STRING25 LastName;
		DataSET(setnums) mynumbers;
END;

allPeople := DATASET([  {1, 'Fred', 'Smith',[1,2,3,4]},
                        {2, 'Joe', 'Blow',[5,6,7]},
                      {3, 'Jane', 'Smith',[1000,10002,30040]}], Layout_Person);

somePeople := allPeople(LastName = 'Smith');

somePeople;