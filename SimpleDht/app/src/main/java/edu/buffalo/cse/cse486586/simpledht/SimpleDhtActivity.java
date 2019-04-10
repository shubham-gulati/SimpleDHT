package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentResolver;
import android.net.Uri;
import android.os.Bundle;
import android.app.Activity;
import android.text.method.ScrollingMovementMethod;
import android.view.Menu;
import android.view.View;
import android.widget.Button;
import android.widget.TextView;

public class SimpleDhtActivity extends Activity {

    public ContentResolver contentResolver;
    private final Uri cpUri  = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_simple_dht_main);
        Button buttonLdump, buttonGdump;
        contentResolver = getContentResolver();

        buttonLdump = (Button) findViewById(R.id.button1);
        buttonGdump = (Button) findViewById(R.id.button2);
        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());
        findViewById(R.id.button3).setOnClickListener(
                new OnTestClickListener(tv, getContentResolver()));


        buttonLdump.setOnClickListener(new View.OnClickListener() {
               @Override
               public void onClick(View v) {
                   //here we will dump all keys in local partition to the node



                }
           }
        );

        buttonGdump.setOnClickListener(new View.OnClickListener() {
               @Override
               public void onClick(View v) {
                   //here we will dump all keys in entire given chord dht



               }
           }
        );



    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_simple_dht_main, menu);
        return true;
    }

}
