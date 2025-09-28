class FloodETL:
    def __init__(self):
        self.base_url = "https://environment.data.gov.uk/flood-monitoring/id"
        self.data_dir = Path("data")
        self.raw_dir = self.data_dir / "raw"
        self.processed_dir = self.data_dir / "processed"
        self.features_dir = self.data_dir / "features"
        
        # Create directories
        for dir_path in [self.raw_dir, self.processed_dir, self.features_dir]:
            dir_path.mkdir(parents=True, exist_ok=True)