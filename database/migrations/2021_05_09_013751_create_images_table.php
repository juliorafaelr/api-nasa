<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

class CreateImagesTable extends Migration
{
    /**
     * Run the migrations.
     *
     * @return void
     */
    public function up()
    {
        Schema::create('images', function(Blueprint $table) {
            $table->uuid('imageid');
            $table->text('extended');
            $table->integer('sol');
            $table->text('attitude');
            $table->text('image_files');
            $table->text('camera');
            $table->text('caption');
            $table->text('sample_type');
            $table->text('date_taken_mars');
            $table->text('credit');
            $table->timestamp('date_taken_utc');
            $table->text('json_link');
            $table->text('drive');
            $table->text('title');
            $table->integer('site');
            $table->text('link');
            $table->timestamp('date_received');
        });
    }

    /**
     * Reverse the migrations.
     *
     * @return void
     */
    public function down()
    {
        Schema::dropIfExists('images');
    }
}
