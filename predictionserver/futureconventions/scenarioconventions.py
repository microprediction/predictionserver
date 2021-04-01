class ScenarioConventions:

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.SUBMISSION_NOISE = 0.001  # Tie-breaking / smoothing noise added to predictions
        self.TRUTH_NOISE = 0.01       # Noise added to truth
        self.NUM_PREDICTIONS = 225
