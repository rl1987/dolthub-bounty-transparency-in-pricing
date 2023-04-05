from enum import Enum

class FileFormat(Enum):
    XML = "XML"
    CSV = "CSV"
    JSON = "JSON"
    XLSX = "XLSX"

class FileSubtype(Enum):
    AURORA = "AURORA"
    
class TypeRecognizer(object):
    def _looks_like_aurora_xml(self, file_path):
        try:
            df = pd.read_xml(file_path)
        except:
            return False
        
        columns = df.columns.to_list()
        
        if len(columns) > 10:
            check_colnames = ['Facility', 'Type', 'Chargecode_DRG_CPT', 'Description', 
                              'Rev', 'CPT', 'NDC', 'Self_Pay', 'Min', 'Max']
            for col_name in check_colnames:
                if not col_name in columns:
                    return False
            
            remaining_colnames = list(set(columns) - set(check_colnames))
            for col_name in remaining_colnames:
                if " " in col_name:
                    return False
                
                if not col_name.startswith("_"):
                    return False
                
                components = col_name.split("_")
                
                if len(components) < 3:
                    return False
                
                if len(components[1]) != 4 and components[-1] != 'Fee':
                    return False
                
                return True
        
        return False
    
    def recognize_format_and_subtype(self, file_path):
        file_format = None
        subtype = None
        
        if file_path.endswith(".xml") or file_path.endswith(".XML"):
            file_format = FileFormat.XML
            
            if self._looks_like_aurora_xml(file_path):
                subtype = FileSubtype.AURORA
        
        return file_format, subtype
