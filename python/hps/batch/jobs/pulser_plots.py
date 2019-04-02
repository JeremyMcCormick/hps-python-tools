from ROOT import TFile, TH1D
from hps.lcio.proc import EventManager, PlotsProcessor
from hps.util.units import MeV

class PulserPlotsProcessor(PlotsProcessor):
        
    def start(self):
        PlotsProcessor.start(self)
        self.ecal_hit_e_h1d = TH1D("Ecal Hit Energy", "Ecal Hit Energy", 700, 0.0, 1400.0)
        self.ecal_hit_time = TH1D("Ecal Hit Time", "Ecal Hit Time", 210, -10.0, 200.0)
        self.ecal_ro_sample1 = TH1D("Ecal ADC Sample 1", "Ecal ADC Sample 1", 250, 100., 250.)
    
    def process(self, event):
        ecal_hits = event.getCollection("EcalCalHits")
        for ecal_hit in ecal_hits:
            ecal_hit_e = ecal_hit.getEnergy() * MeV
            self.ecal_hit_e_h1d.Fill(ecal_hit_e)
            t = ecal_hit.getTime()
            self.ecal_hit_time.Fill(t)
        ecal_ro_hits = event.getCollection("EcalReadoutHits")
        for ecal_ro_hit in ecal_ro_hits:
            adc_values = ecal_ro_hit.getADCValues() # len = 50
            self.ecal_ro_sample1.Fill(adc_values[0])            
    
    def end(self):
        PlotsProcessor.end(self)
        