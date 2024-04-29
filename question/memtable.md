### Why doesn't the memtable provide a delete API?


      因为memtable会以两种会以mutable memtable和immutable memtabls这两种形式存在。在进行get操作时，会查找提供的key，优先从查找mutable memtable，如果没有找到，就会从immutable memtabls按时间顺序，从最近到最远开始遍历查找，如果在找到了就立刻停止遍历返回。
      delete操作和get取操作类似，不同之处在于。在删除在任意一个memtable找到并删除之后，不是停止遍历而是继续下去，把提供的key在所有memtable都删除，以此来保证get操作不会读取到**旧值**。
      为了避免delete操作的耗时操作，所以使用**删除墓碑**的形式，put一个空值来代表删除