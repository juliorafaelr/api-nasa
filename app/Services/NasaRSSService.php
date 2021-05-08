<?php

namespace App\Services;

use Illuminate\Support\Facades\Http;

class NasaRSSService
{
    protected string $endPoint;

    protected ?string $body;

    protected ?int $page;

    private ?array $params;

    /**
     * @var array|mixed
     */
    private $perPage;

    /**
     * @var array|mixed
     */
    private $resultCount;

    public function __construct()
    {
        $this->endPoint = env('END_POINT');

        $this->body = null;
    }

    /**
     *
     * @param array $params
     *
     * @return self
     */
    public function getImages(array $params = []): self
    {
        if (!empty($params)) {
            $this->params = $params;
        }

        if (!empty($this->page)) {
            $this->params['page'] = $this->page;
        }

        unset($this->body);

        $this->body = Http::get($this->endPoint, $this->params);

        $body = json_decode($this->body, true);

        $this->page = data_get($body, 'page');

        $this->perPage = data_get($body, 'per_page');

        $this->resultCount = count(data_get($body, 'images'));

        unset($body);

        return $this;
    }

    /**
     * @return string
     */
    public function getBody(): string
    {
        return $this->body;
    }

    /**
     * @return array
     */
    public function toArray(): array
    {
        return json_decode($this->getBody(), true);
    }

    /**
     * @return bool
     */
    public function nextPage(): bool
    {
        if ($this->resultCount < $this->perPage) {
            $this->body = null;

            return false;
        }

        $this->page++;

        $this->getImages();

        return true;
    }
}
