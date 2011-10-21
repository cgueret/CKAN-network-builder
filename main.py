#!/usr/bin/python
'''
Note: this script requires Python 3
'''
import json
import http.client

# Content of the network
network = {}

# Map the topics to indexes
index_topic = {
    '__unspecified__':1,
    'media':2,
    'geographic':3,
    'publications':4,
    'usergeneratedcontent':5,
    'government':6,
    'crossdomain' : 7,
    'lifesciences': 8
}

def links_to_weight(size):
    '''
    Map the number of links to different weights
    '''
    if size < 1*1000:
        return 1.0 # Thin
    elif size < 100*1000:
        return 2.0 # Medium
    else:
        return 4.0 # Thick

def size_to_scale(size):
    '''
    Map the size of a node to a scale factor
    '''
    # Map the size to different colors
    if size < 10*1000:
        return 1 # Very small
    elif size < 500*1000:
        return 2 # Small
    elif size < 10*1000*1000:
        return 3 # Medium
    elif size < 1*1000*1000*1000:
        return 4 # Large
    else:
        return 5 # Very large

    
def get_package(package, conn):
    # If the node is already known, return it
    if package in network:
        return network[package]

    # Get all the data
    conn.request("GET", "/api/rest/package/"+package)
    response = conn.getresponse().read().decode('utf-8')
    res = json.loads(response)

    # Extract what we need
    data = {}
    if 'extras' in res:
        data = res['extras']

	# Get the number of triples
    triples = 0
    if 'triples' in data:
        try:
            triples = int(data['triples'])
        except:
            # There was an error parsing the number, assume 0
            pass

    # If the package has less than 1000 triples, ignore it
    if triples < 1000:
        return -1;

    # If the package has no links, ignore it
    # Commented as the filtering is done on actual links declared
    #if 'tags' in res:
    #    if 'lodcloud.nolinks' in res['tags']:
    #        return -1

    # Find a topic
    topic = '__unspecified__'
    if 'tags' in res:
        for tag in res['tags']:
            if tag in index_topic:
                topic = tag
    
    # Find a nice name for the node
    title = package
    if 'shortname' in data:
        title = data['shortname']
    elif 'title' in res:
        title = res['title']
    title = "".join([c for c in title if ord(c) < 128])
    if len(title) < 2:
        title = package

    # Store the node
    network[package] = {}
    network[package]['data'] = data
    network[package]['title'] = title
    network[package]['triples'] = triples
    network[package]['arcs'] = {}
    network[package]['incoming'] = 0
    network[package]['topic'] = index_topic[topic]
    
    # Return the index
    return network[package]

def main():
    '''
    Create a .net file, using the Pajek format, representing
    the content of the LOD Cloud. All the data is fetched
    from TheDataHub (formerly CKAN).
    More information about the cloud available on
    http://lod-cloud.net/
    '''
    # Open a connection to CKAN
    conn = http.client.HTTPConnection("thedatahub.org")

    # Get package list
    conn.request("GET", "/api/search/package?groups=lodcloud&limit=500")
    data = conn.getresponse().read().decode('utf-8')
    res = json.loads(data)
    packages = res['results']
    print ("Got the name of %d packages" % len(packages))

    # Process each package
    for package in packages:

        # Get the node
        node = get_package(package,conn)
        if node == -1:
            continue;

        print ("Add",package)
        
        # Store its connections
        for entry in node['data']:
            parts = entry.split('links:')
            if len(parts) == 2:
                target = get_package(parts[1],conn)
                if target == -1:
                    continue;
                print ("\t->",parts[1])
                try:
                    nb_links = int(node['data'][entry])
                    if nb_links >= 50:
                        node['arcs'][parts[1]] = nb_links
                        target['incoming'] = target['incoming'] + 1
                except ValueError:
                    # It's likely no link count has been indicated, ignore the link
                    pass
                    
    # Close the connection        
    conn.close()

    # Delete all the nodes not connected
    delete = []
    for package in network.keys():
        if len(network[package]['arcs']) == 0 and network[package]['incoming'] == 0:
            delete.append(package)
    for package in delete:
        del network[package]

    # Assign ids
    i = 1
    for package in network.keys():
        network[package]['id'] = i
        i = i + 1
                   
    # Save the Pajek file
    net_file = open ('lod-cloud.net','w')
    print ("*Network lod-cloud.net",file=net_file)
    print ("*Vertices",len(network),file=net_file)
    for package in network.keys():
        scale = size_to_scale(network[package]['triples'])
        print (network[package]['id'],"\"%s\"" % network[package]['title'],"0.0 0.0 0.0","x_fact",scale,"y_fact",scale,"ic Orange bc Black",file=net_file)
    print ("*Arcs",file=net_file)
    for start in network.keys():
        for (end,size) in network[start]['arcs'].items():
            print (network[start]['id'],network[end]['id'],links_to_weight(size),"c Black",file=net_file)
    print ("*Partition topics",file=net_file)
    print ("*Vertices",len(network),file=net_file)
    for package in network.keys():
        print ('',network[package]['topic'],sep="\t",file=net_file)
    net_file.close()

    # Save the CSV files for Gephi
    nodes_file = open ('lod-cloud-nodes.csv','w')
    edges_file = open ('lod-cloud-edges.csv','w')
    print ("Id","Label","Weight","Topic",sep=',',file=nodes_file)
    print ("Source","Target","Type","Weight",sep=',',file=edges_file)
    for package in network.keys():
        print (network[package]['id'],network[package]['title'],float(size_to_scale(network[package]['triples'])),network[package]['topic'],sep=',',file=nodes_file)
        for (end,size) in network[package]['arcs'].items():
            print (network[package]['id'],network[end]['id'],'Directed',links_to_weight(size),sep=',',file=edges_file)
    nodes_file.close()
    edges_file.close()
    
if __name__ == '__main__':
    main()

