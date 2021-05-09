<?php

namespace App\Commands;

use Illuminate\Console\Scheduling\Schedule;
use LaravelZero\Framework\Commands\Command;
use Illuminate\Support\Facades\DB;

class GetImages extends Command
{
    /**
     * The signature of the command.
     *
     * @var string
     */
    protected $signature = 'nasa:get-images';

    /**
     * The description of the command.
     *
     * @var string
     */
    protected $description = 'get nasa images';

    /**
     * Execute the console command.
     *
     * @return mixed
     */
    public function handle(): int
    {
        $images = DB::table('images')->get()->toArray();

        dump($images);

        return 0;
    }

    /**
     * Define the command's schedule.
     *
     * @param Schedule $schedule
     * @return void
     */
    public function schedule(Schedule $schedule)
    {
        // $schedule->command(static::class)->everyMinute();
    }
}
