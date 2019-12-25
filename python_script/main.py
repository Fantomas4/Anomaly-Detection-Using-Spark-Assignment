file = open("../data201920.csv")

counter = 0

for line in file:
    counter += 1

    if "," not in line:
        print("Case 1: ", line)
    elif line[0] == ",":
        print("Case 2: ", line)
    elif line[len(line) - 1] == ",":
        print("Case 3: ", line)

print("Total count: ", counter)