import java.nio.file.*;
import java.security.*;
import java.util.*;
import org.rocksdb.*;
public class JarProbe {
  static String hash(byte[] b) throws Exception {
    StringBuilder s=new StringBuilder();
    for(byte x:MessageDigest.getInstance("SHA-256").digest(b))s.append(String.format("%02x",x & 255));
    return s.toString();
  }
  public static void main(String[] args) throws Exception {
    RocksDB.loadLibrary();
    System.out.println("class_origin="+RocksDB.class.getProtectionDomain().getCodeSource().getLocation());
    System.out.println("version="+RocksDB.rocksdbVersion());
    for(String line:Files.readAllLines(Paths.get("/proc/self/maps"))) {
      if(line.contains("librocksdbjni") && line.contains("/")) {
        String path=line.substring(line.indexOf('/')).replace(" (deleted)","");
        System.out.println("native_path="+path+" native_sha256="+hash(Files.readAllBytes(Paths.get(path))));
        break;
      }
    }
    DBOptions opt=new DBOptions(); List<ColumnFamilyDescriptor> desc=new ArrayList<>();
    OptionsUtil.loadOptionsFromFile(args[0],Env.getDefault(),opt,desc);
    Statistics stats=new Statistics();opt.setStatistics(stats);opt.setCreateIfMissing(true);
    List<ColumnFamilyHandle> handles=new ArrayList<>();
    RocksDB db=RocksDB.open(opt,args[1],desc,handles);
    byte[] value=new byte[1024];new Random(1).nextBytes(value);
    for(int round=0;round<2;round++) {
      for(int i=0;i<10000;i++)db.put(handles.get(0),Integer.toString(i).getBytes("UTF-8"),value);
      db.flush(new FlushOptions().setWaitForFlush(true));
    }
    db.compactRange();
    for(ColumnFamilyHandle h:handles)h.close();db.close();
    System.out.println(stats.toString());
    for(ColumnFamilyDescriptor d:desc)d.getOptions().close();opt.close();stats.close();
  }
}
