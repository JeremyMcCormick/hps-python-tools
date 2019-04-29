from string import Template

auger_tmpl = """<Request>
<Email email="${user}" request="false" job="false"/>
<Project name="hps"/>
<Track name="simulation"/>
<Name name="${name}"/>
<Command><![CDATA[
${command}
]]></Command>
<Job>
</Job>
</Request>"""

#<Output src="${output_src}" dest="${output_dest}"/>
#<Stderr dest="${stderr_dest}"/>
#<Stdout dest="${stdout_dest}"/>

class AugerWriter:
    
    def __init__(self, parameters):
        self.parameters = parameters
        
    def write(self, outfile = 'auger.xml'):
        tmpl = Template(auger_tmpl)
        subs = tmpl.substitute(self.parameters)
        print subs
        
        #with open(outfile, 'w') as augerfile:

if __name__ == '__main__':
    p = {
            'user': 'jeremym@jlab.org',
            'name': 'DummyJob',
            'command': "echo Hello Auger"           
    }    
    writer = AugerWriter(p)
    writer.write()