import configparser

class Config:
    """
        App Configuration Class
    """
    def __init__(self, filename: str):
        """
            Read configuration for App

            Params
            * filename including path

        """
        self.config = configparser.ConfigParser()
        self.config.read_file(open(filename))
    
    def get_extension(self) -> str:
        return self.config.get("file-config","extension")

    def get_prefix(self) -> str:
        return self.config.get("file-config","prefix")

    def get_dbpath(self) -> str:
        return self.config.get("database-config","db_path")
    
    def get_dbname(self) -> str:
        return self.config.get("database-config","db_name")
    
    def get_area_filename(self) -> str:
        return self.config.get("support-config","area_file")
    
    def get_area_path(self) -> str:
        return self.config.get("support-config","suppor_path")
    
    def get_input_path(self) -> str:
        return self.config.get("path-config","input_path")
    
    def get_output_path(self) -> str:
        return self.config.get("path-config","output_path")

    def get_signal(self) -> str:
        return self.config.get("process","signal")
    