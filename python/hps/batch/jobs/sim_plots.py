"""
Basic sim comparison plots.

@author Jeremy McCormick (SLAC)
"""

import sys
from ROOT import TFile, TH1D
from hps.lcio.event_proc import EventManager, PlotsProcessor

# Multiple for conversion from GeV to MeV
MeV = 1000.

# TODO: Inherit from PlotsProcessor
class SimPlotsProcessor(PlotsProcessor):
       
    def start(self):
        self.plotfile = TFile(self.plot_file_name, "NEW")

        self.ecal_hit_e_h1d = TH1D("Ecal Hit Energy", "Ecal Hit Energy", 1000, 0.0, 100.0)
        self.ecal_n_hits_h1d = TH1D("Ecal Num Hits per Event", "Ecal Num Hits per Event", 50, 0.0, 50.0)

        self.tracker_edep_h1d = TH1D("Tracker Hit Edep", "Tracker Hit Edep", 100, 0.0, 10.0)
        self.tracker_n_hits_h1d = TH1D("Tracker Num Hits per Event", "Tracker Num Hits per Event", 20, 0., 20.)
        
        self.sim_particle_count_h1d = TH1D("Sim Particles per Event", "Sim Particles per Event", 200, 0., 200.)
        self.sim_particle_e_h1d = TH1D("Sim Particle Energy", "Sim Particle Energy", 1200, 0., 1200.)
        self.sim_particle_low_e_h1d = TH1D("Sim Particle Energy (<200 MeV)", "Sim Particle Energy (<200 MeV)", 1000, 0., 200.)
    
    # ['EcalHits', 'MCParticle', 'TrackerHits', 'TrackerHitsECal']
    # TODO: -Plot 2D of TrackerHitsEcal XY for scoring hits
    #       -Plot all MCParticle data
    #       -Number of Ecal hit contribs
    #       -Ecal hit positions in XY
    #       -Tracker hit positions in XY
    #       -Tracker hit positions in Z
    #       -Tracker hit time
    #       -Ecal hit contrib energy, position, etc.
    #       -Tracker low E to show beginning of distribution
    def process(self, event):
        ecal_hits = event.getCollection("EcalHits")
        n_ecal_hits = 0
        for ecal_hit in ecal_hits:
            ecal_hit_e = ecal_hit.getEnergy() * MeV
            self.ecal_hit_e_h1d.Fill(ecal_hit_e)
            n_ecal_hits += 1
        self.ecal_n_hits_h1d.Fill(n_ecal_hits)
        
        tracker_hits = event.getCollection("TrackerHits")
        n_tracker_hits = 0
        for tracker_hit in tracker_hits:
            tracker_hit_edep = tracker_hit.getEDep() * MeV
            self.tracker_edep_h1d.Fill(tracker_hit_edep)
            n_tracker_hits += 1
        self.tracker_n_hits_h1d.Fill(n_tracker_hits)
        
        mcparticles = event.getCollection("MCParticle")
        n_sim_particles = 0
        pdgs = {}
        for mcp in mcparticles:
            e = mcp.getEnergy() * MeV
            if mcp.getGeneratorStatus() == 0 or mcp.getParents().size() > 0L:
                n_sim_particles += 1
                self.sim_particle_e_h1d.Fill(e)
                if (e < 200.):
                    self.sim_particle_low_e_h1d.Fill(e)
            pdg = mcp.getPDG()
            if pdg not in pdgs:
                pdgs[pdg] = 0
            pdgs[pdg] += 1
        self.sim_particle_count_h1d.Fill(n_sim_particles)
           
if __name__ == "__main__":
    plot_file_name = sys.argv[-1]
    files = sys.argv[1:len(sys.argv) - 1]
    print("input files: " + str(files))
    print("output plot file: " + plot_file_name)
    processors = [SimPlotsProcessor("MySimPlotsProcessor", plot_file_name)]
    mgr = EventManager(processors, files)
    mgr.process() 
