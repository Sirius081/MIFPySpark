def jsim(a, b):
    count = 0.0
    for i in a:
        if i in b:
            count += 1
    return count / (len(set(a + b)))


print jsim([1, 2, 3], [2, 3, 4])
