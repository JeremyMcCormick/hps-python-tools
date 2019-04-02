import pyLCIO as lcio
from ROOT import TFile

class Processor:

    #def __init__(self, name = "Processor"):
    #    self.name = name

    def start(self):
        pass
        #print(self.name + " - startOfJob") 

    def process(self, event):
        pass
        #print(self.name + " - processEvent " + str(event.getEventNumber())) 

    def end(self):
        pass
        #print(self.name + " - endOfJob")
        
class PlotsProcessor(Processor):
        
    def __init__(self, plot_file_name):
        self.plot_file_name = plot_file_name

    def start(self):
        self.plotfile = TFile(self.plot_file_name, "NEW")
        
    def end(self):
        self.plotfile.Write()
        self.plotfile.Close()        

class EventManager:

    def __init__(self, processors, files):
        self.processors = processors
        self.files = files

    def _start(self):
        for processor in self.processors:
            processor.start() 
   
    def _end(self):
        for processor in self.processors:
            processor.end() 

    def _processEvent(self, event):
        for processor in self.processors:
            processor.process(event) 

    def process(self):
        self._start()
        rdr = lcio.IOIMPL.LCFactory.getInstance().createLCReader()
        n_events_processed = 0
        for lcio_file in self.files:
            rdr.open(lcio_file)
            evt = rdr.readNextEvent()
            while evt != None:
                self._processEvent(evt)
                n_events_processed += 1
                evt = rdr.readNextEvent()
            rdr.close()
        self._end()
        print "Processed " + str(n_events_processed) + " events"

"""
if __name__ == "__main__":
    processors = [Processor("DummyProcessor")]
    files = ["slicEvents.slcio"]
    mgr = EventManager(processors, files)
    mgr.processEvents()
"""