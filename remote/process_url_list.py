def binding_id_from_url(url):
    parts = url.split('/')
    binding_id_index = parts.index('binding') + 1 # throws ValueError
    return parts[binding_id_index] # throws IndexError

def filename_from_url(url):
    return url.split('/')[-1]

def path_from_url(url):
    binding_id = binding_id_from_url(url)
    return '/'.join((binding_id[:i+1] for i in range(len(binding_id)+1)))

while True:
    try:
        url = input().strip()
    except EOFError:
        break
    print(url + '\t' + path_from_url(url) + '/' + filename_from_url(url))
