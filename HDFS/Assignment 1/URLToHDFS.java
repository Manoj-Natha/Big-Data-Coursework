package JavaHDFS.JavaHDFS;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.util.Progressable;

public class URLToHDFS {

    private static void downloadUsingStream(String urlStr, String dst) throws IOException{
        URL url = new URL(urlStr);
        InputStream in = new BufferedInputStream(url.openStream());
        
        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(URI.create(dst), conf);
        OutputStream out = fs.create(new Path(dst), new Progressable() {
          public void progress() {
            System.out.print(".");
          }
        });
        
        IOUtils.copyBytes(in, out, 4096, true);
        in.close();
        out.close();
        
        // File Decompressor 
        Path inputPath = new Path(dst);
	    CompressionCodecFactory factory = new CompressionCodecFactory(conf);
	    CompressionCodec codec = factory.getCodec(inputPath);
	    if (codec == null) {
	      System.err.println("No codec found for " + dst);
	      System.exit(1);
	    }

	    String outputUri =
	      CompressionCodecFactory.removeSuffix(dst, codec.getDefaultExtension());

	    in = null;
	    out = null;
	    try {
		      in = codec.createInputStream(fs.open(inputPath));
		      out = fs.create(new Path(outputUri));
		      IOUtils.copyBytes(in, out, conf);
		    } finally {
		      IOUtils.closeStream(in);
		      IOUtils.closeStream(out);
		      fs.delete(inputPath);
		      
		    }
	    
    }
	public static void main(String[] args) {
		String url1 = "http://www.utdallas.edu/~axn112530/cs6350/lab2/input/20417.txt.bz2";
		String url2 = "http://www.utdallas.edu/~axn112530/cs6350/lab2/input/5000-8.txt.bz2";
		String url3 = "http://www.utdallas.edu/~axn112530/cs6350/lab2/input/132.txt.bz2";
		String url4 = "http://www.utdallas.edu/~axn112530/cs6350/lab2/input/1661-8.txt.bz2";
		String url5 = "http://www.utdallas.edu/~axn112530/cs6350/lab2/input/972.txt.bz2";
		String url6 = "http://www.utdallas.edu/~axn112530/cs6350/lab2/input/19699.txt.bz2";
		
		try {
             
            downloadUsingStream(url1, "20417.txt.bz2");
            downloadUsingStream(url2, "5000-8.txt.bz2");
            downloadUsingStream(url3, "132.txt.bz2");
            downloadUsingStream(url4, "1661-8.txt.bz2");
            downloadUsingStream(url5, "972.txt.bz2");
            downloadUsingStream(url6, "19699.txt.bz2");
        } catch (IOException e) {
            e.printStackTrace();
        }

	}

}
