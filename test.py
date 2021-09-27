import file_check as fc
import quality_check

# Test 
def test_file_check():

    location = "/media/sf_compartido/spark/"
    pfx = "data_file_"
    ex = "csv"

    test_dict = {
        "directory":"data_file_20210618152931.csv",
        "empty":"data_file_20210728182844.csv",
        "bad_extension":"data_file_20210528182844.txt",
        "bad_prefix":"dat_file_20210528182844.csv",
        "incomplete_date":"data_file_202105281828.csv",
        "bad_date":"data_file_202114281828.csv",
        "non_empty":"data_file_20210528182844.csv"
    }
    
    for key,value in test_dict.items():
        file_name = value
        st, er = fc.check_file_constraints(location,file_name, pfx, ex)
        print("test " + key + " - " + "%s, %s"% (str(st),er))

test_file_check()

def quality_check():
    s = ' Pingüino: [ ]  { } / \ ? ¿ $ ! + [] # % & ñ Ñ ... moño'    
    print(s + " -> " + clean_descriptive(s))

    # Test numbers
    numbers = [ "+080 90xx09090 +90  90909090",
            "+080 93909090 +90  90009090 +90 00000000",
            "+99 999990009 90 0000000",
            "+99 99xx9990009 +99 999xx990009",
            "   +99 999990009     ",
            " +0 999990009 +01 9912310009"]
    for x in numbers:
        tok = clean_phone(x)
        print(x + " ->  [" + ",".join(tok) + "]")

    areas_path = "/media/sf_compartido/spark/"
    areas_file = "Areas_in_blore.xlsx"

    # Validate location
    df = read_areas(areas_path, areas_file)
    
    location = "Vidyanagara"
    s = validate_location(location,df)
    print (location + " --> " + s )

    location = "Vidyanagar"
    s = validate_location(location,df)
    print (location + " --> " + s )

    

#df.select("temp_phone","contact_number_1", "contact_number_2").show(truncate=False)
        #df.select("address").show(truncate=False)
        #df.select("reviews_list").show(truncate=False)
        

