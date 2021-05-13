old = open("raw_test_data_0.txt", "r")
new = open("test_data_0.txt", "w")

lines = old.readlines()
newlines = []

for line in lines:
    newline = line.replace(",", "").replace(".", "").replace(";", "").replace("'", "")
    newlines.append(newline)

new.writelines(newlines)

old = open("raw_test_data_1.txt", "r")
new = open("test_data_1.txt", "w")

lines = old.readlines()
newlines = []

for line in lines:
    newline = line.replace(",", "").replace(".", "").replace(";", "").replace("'", "")
    newlines.append(newline)

new.writelines(newlines)
