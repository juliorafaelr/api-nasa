<?php

namespace App\Commands;

use App\Services\NasaRSSService;
use Illuminate\Console\Scheduling\Schedule;
use LaravelZero\Framework\Commands\Command;

class SaveImages extends Command
{
    /**
     * The signature of the command.
     *
     * @var string
     */
    protected $signature = 'nasa:save-images';

    /**
     * The description of the command.
     *
     * @var string
     */
    protected $description = 'Display an inspiring quote';

    /**
     * Execute the console command.
     *
     * @return mixed
     */
    public function handle(NasaRSSService $nasaRSSService)
    {
        $params = [
            'feed' => 'raw_images',
            'category' => 'mars2020',
            'feedtype' => 'json',
            'num' => 100,
            'page' => 0,
            'order' => 'sol desc',
            'condition_2' => '64:sol:gte',
            'extended' => 'sample_type::full'
        ];

        dump($nasaRSSService->getImages($params)->toArray());

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
