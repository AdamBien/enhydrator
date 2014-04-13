function execute(entry) {
    var list = new java.util.ArrayList();
    var value = entry.value;
    list.add(entry.changeValue("{value}'"));
    return list;
}

