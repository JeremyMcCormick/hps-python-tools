import pyLCIO as lcio

class Processor:

    def __init__(self, name = "Processor"):
        self.name = name

    def startOfJob(self):
        print(self.name + " - startOfJob") 

    def processEvent(self, event):
        print(self.name + " - processEvent " + str(event.getEventNumber())) 

    def endOfJob(self):
        print(self.name + " - endOfJob")

class EventManager:

    def __init__(self, processors, files):
        self.processors = processors
        self.files = files

    def startOfJob(self):
        for processor in self.processors:
            processor.startOfJob() 
   
    def endOfJob(self):
        for processor in self.processors:
            processor.endOfJob() 

    def processEvent(self, event):
        for processor in self.processors:
            processor.processEvent(event) 

    def processEvents(self):
        self.startOfJob()
        rdr = lcio.IOIMPL.LCFactory.getInstance().createLCReader()
        n_events_processed = 0
        for lcio_file in self.files:
            rdr.open(lcio_file)
            evt = rdr.readNextEvent()
            while evt != None:
                self.processEvent(evt) 
                n_events_processed += 1
                evt = rdr.readNextEvent()
            self.endOfJob()
            rdr.close()
        self.endOfJob()
        print "Processed " + str(n_events_processed) + " events"

if __name__ == "__main__":
    processors = [Processor("DummyProcessor")]
    files = ["slicEvents.slcio"]
    mgr = EventManager(processors, files)
    mgr.processEvents()
